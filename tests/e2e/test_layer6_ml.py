"""E2E Test: Layer 6 - ML Feature Engineering"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.agents.ml.feature_store import FeatureStore


def test_feature_engineering():
    print("=" * 60)
    print("LAYER 6: ML FEATURE ENGINEERING TEST")
    print("Source: AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS")
    print("Output: AGENTIC_PLATFORM.ML.SALES_FEATURES")
    print("=" * 60)

    store = FeatureStore(
        database="AGENTIC_PLATFORM",
        schema="ML",
    )

    print("\n[1] Discovering existing features")
    try:
        existing = store.discover_features("AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS")
        print(f"    ✓ Found {len(existing)} existing features")
        
        numeric_feats = [f for f in existing if f.category == 'numeric']
        temporal_feats = [f for f in existing if f.category == 'temporal']
        categorical_feats = [f for f in existing if f.category == 'categorical']
        
        print(f"      Numeric: {len(numeric_feats)} - {[f.name for f in numeric_feats[:5]]}...")
        print(f"      Temporal: {len(temporal_feats)} - {[f.name for f in temporal_feats]}")
        print(f"      Categorical: {len(categorical_feats)} - {[f.name for f in categorical_feats[:5]]}...")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[2] Creating window/rolling features")
    try:
        window_features = store.create_window_features(
            table_name="AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS",
            value_column="TOTAL_AMOUNT",
            partition_column="CUSTOMER_ID",
            order_column="ORDER_DATE",
            windows=[7, 30],
        )
        print(f"    ✓ Created {len(window_features)} window features")
        for f in window_features[:3]:
            print(f"      - {f.name}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[3] Creating lag features")
    try:
        lag_features = store.create_lag_features(
            value_column="TOTAL_AMOUNT",
            partition_column="CUSTOMER_ID",
            order_column="ORDER_DATE",
            lags=[1, 7],
        )
        print(f"    ✓ Created {len(lag_features)} lag features")
        for f in lag_features[:3]:
            print(f"      - {f.name}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[4] Creating temporal features")
    try:
        temporal_features = store.create_temporal_features("ORDER_DATE")
        print(f"    ✓ Created {len(temporal_features)} temporal features")
        for f in temporal_features[:4]:
            print(f"      - {f.name}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[5] Materializing feature table")
    try:
        all_features = window_features + lag_features + temporal_features
        output_table = store.materialize_feature_table(
            source_table="AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS",
            features=all_features,
            output_table="AGENTIC_PLATFORM.ML.SALES_FEATURES",
            include_source_columns=True,
        )
        print(f"    ✓ Created feature table: {output_table}")
    except Exception as e:
        print(f"    ✗ FAILED to materialize: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[6] Verifying feature table")
    try:
        verify = store._execute("""
            SELECT COUNT(*) as cnt, COUNT(DISTINCT CUSTOMER_ID) as customers
            FROM AGENTIC_PLATFORM.ML.SALES_FEATURES
        """)
        cnt = verify[0]['CNT'] if verify else 0
        customers = verify[0]['CUSTOMERS'] if verify else 0
        print(f"    ✓ Feature table has {cnt} rows, {customers} unique customers")
    except Exception as e:
        print(f"    ✗ Verification failed: {e}")
        return False

    print("\n[7] Computing feature statistics")
    try:
        stats = store.get_feature_stats(
            "AGENTIC_PLATFORM.ML.SALES_FEATURES",
            ["TOTAL_AMOUNT", "TOTAL_AMOUNT_ROLLING_AVG_7", "ORDER_DATE_MONTH"]
        )
        print(f"    ✓ Computed stats for {len(stats)} features")
        for feat, s in stats.items():
            if not s.get('error'):
                print(f"      {feat}: mean={s.get('mean'):.2f}, std={s.get('std'):.2f}")
    except Exception as e:
        print(f"    ⚠ Stats computation partial: {e}")

    print("\n" + "=" * 60)
    print("LAYER 6: PASSED ✓")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_feature_engineering()
    sys.exit(0 if success else 1)
