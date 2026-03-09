"""
ML Pipeline E2E Test: Train and Register model on drilling data

Uses XGBoost native training with Snowpark ML Registry
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
ML_SCHEMA = "ML"
RAW_SCHEMA = "RAW"
CONNECTION_NAME = "my_snowflake"


def run_ml_pipeline():
    """Run the complete ML pipeline."""
    import xgboost as xgb
    from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
    from sklearn.model_selection import train_test_split
    from snowflake.snowpark import Session

    print("\n" + "=" * 70)
    print("ML PIPELINE E2E TEST - STUCK PIPE PREDICTION")
    print("=" * 70)

    session = Session.builder.configs(
        {
            "connection_name": CONNECTION_NAME,
            "database": DATABASE,
            "schema": ML_SCHEMA,
        }
    ).create()

    session.sql(f"USE DATABASE {DATABASE}").collect()
    session.sql(f"USE SCHEMA {ML_SCHEMA}").collect()

    results = {}

    try:
        print("\n" + "=" * 60)
        print("STEP 1: Load Training Data")
        print("=" * 60)

        query = f"""
        SELECT
            RATE_OF_PENETRATION_M_H AS ROP,
            WEIGHT_ON_BIT_KKGF AS WOB,
            AVERAGE_HOOKLOAD_KKGF AS HOOKLOAD,
            AVERAGE_SURFACE_TORQUE_KN_M AS TORQUE,
            AVERAGE_STANDPIPE_PRESSURE_KPA AS PRESSURE,
            AVERAGE_ROTARY_SPEED_RPM AS RPM,
            HOLE_DEPTH_MD_M AS DEPTH,
            CASE
                WHEN RATE_OF_PENETRATION_M_H = 0 AND WEIGHT_ON_BIT_KKGF > 5 AND AVERAGE_SURFACE_TORQUE_KN_M > 5 THEN 1
                WHEN RATE_OF_PENETRATION_M_H < 1 AND AVERAGE_HOOKLOAD_KKGF > 150 AND AVERAGE_SURFACE_TORQUE_KN_M > 10 THEN 1
                ELSE 0
            END AS IS_STUCK
        FROM {DATABASE}.{RAW_SCHEMA}.DRILLING_TIME SAMPLE (100000 ROWS)
        WHERE RATE_OF_PENETRATION_M_H IS NOT NULL
          AND WEIGHT_ON_BIT_KKGF IS NOT NULL
          AND AVERAGE_HOOKLOAD_KKGF IS NOT NULL
          AND AVERAGE_SURFACE_TORQUE_KN_M IS NOT NULL
        """

        df = session.sql(query).to_pandas()
        print(f"✅ Loaded {len(df):,} rows")
        print(f"   Stuck events: {df['IS_STUCK'].sum()} ({100 * df['IS_STUCK'].mean():.2f}%)")
        results["load_data"] = True

        print("\n" + "=" * 60)
        print("STEP 2: Train XGBoost Model (Local)")
        print("=" * 60)

        feature_cols = ["ROP", "WOB", "HOOKLOAD", "TORQUE", "PRESSURE", "RPM", "DEPTH"]
        df_clean = df.dropna(subset=feature_cols)

        X = df_clean[feature_cols]
        y = df_clean["IS_STUCK"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        print(f"   Train: {len(X_train):,}, Test: {len(X_test):,}")

        model = xgb.XGBClassifier(
            n_estimators=50,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
        )

        model.fit(X_train, y_train)
        print("✅ Model trained locally")
        results["train_model"] = True

        print("\n" + "=" * 60)
        print("STEP 3: Evaluate Model")
        print("=" * 60)

        y_pred = model.predict(X_test)

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, zero_division=0)
        recall = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)

        metrics = {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
        }

        print("✅ Evaluation:")
        print(f"   Accuracy:  {accuracy:.4f}")
        print(f"   Precision: {precision:.4f}")
        print(f"   Recall:    {recall:.4f}")
        print(f"   F1 Score:  {f1:.4f}")

        importance = dict(zip(feature_cols, model.feature_importances_))
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:3]
        print(f"   Top features: {[f'{k}={v:.3f}' for k, v in top_features]}")
        results["evaluate_model"] = True

        print("\n" + "=" * 60)
        print("STEP 4: Register Model in Snowflake")
        print("=" * 60)

        from snowflake.ml.registry import Registry

        registry = Registry(session=session, database_name=DATABASE, schema_name=ML_SCHEMA)

        model_name = "STUCK_PIPE_DETECTOR"
        version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"   Registering {model_name}/{version}...")

        sample_input = X_train.head(100)

        registry.log_model(
            model=model,
            model_name=model_name,
            version_name=version,
            sample_input_data=sample_input,
            metrics=metrics,
            comment="Stuck pipe prediction - Volve drilling data (XGBoost)",
        )

        print(f"✅ Registered: {DATABASE}.{ML_SCHEMA}.{model_name}")
        print(f"   Version: {version}")
        results["register_model"] = True

        models = registry.show_models()
        print(f"   Total models in registry: {len(models)}")

        print("\n" + "=" * 60)
        print("STEP 5: Test Model Inference via Registry")
        print("=" * 60)

        try:
            mv = registry.get_model(model_name).version(version)

            test_sample = X_test.head(10)
            predictions = mv.run(test_sample, function_name="predict")

            print("✅ Registry inference works:")
            for i in range(min(5, len(predictions))):
                actual = y_test.iloc[i]
                pred = predictions.iloc[i, 0]
                match = "✓" if actual == pred else "✗"
                print(f"   {match} Actual: {actual}, Predicted: {pred}")
            results["inference"] = True
        except Exception as e:
            print(f"⚠️  Registry inference: {str(e)[:80]}")
            results["inference"] = False

    except Exception as e:
        import traceback

        print(f"\n❌ Pipeline failed: {str(e)}")
        traceback.print_exc()
    finally:
        session.close()

    print("\n" + "=" * 70)
    print("ML PIPELINE SUMMARY")
    print("=" * 70)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Results: {passed}/{total} steps passed")
    for step, status in results.items():
        print(f"  {'✅' if status else '❌'} {step}")

    return results


if __name__ == "__main__":
    run_ml_pipeline()
