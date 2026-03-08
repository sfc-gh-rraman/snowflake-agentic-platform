"""Feature store agent - feature discovery and engineering."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class FeatureDefinition:
    name: str
    expression: str
    data_type: str
    description: Optional[str] = None
    category: str = "numeric"
    is_derived: bool = False
    source_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "expression": self.expression,
            "data_type": self.data_type,
            "description": self.description,
            "category": self.category,
            "is_derived": self.is_derived,
            "source_columns": self.source_columns,
        }


class FeatureStore:
    """Feature discovery, engineering, and management."""

    def __init__(
        self,
        connection_name: Optional[str] = None,
        database: str = "AGENTIC_PLATFORM",
        schema: str = "ML",
    ):
        self.connection_name = connection_name or os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        self.database = database
        self.schema = schema
        self._session = None

    @property
    def session(self):
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self):
        if os.path.exists("/snowflake/session/token"):
            from snowflake.snowpark import Session
            return Session.builder.getOrCreate()
        else:
            import snowflake.connector
            conn = snowflake.connector.connect(connection_name=self.connection_name)
            return conn

    def _execute(self, sql: str) -> List[Dict]:
        if hasattr(self.session, 'sql'):
            result = self.session.sql(sql).collect()
            return [dict(row.asDict()) for row in result]
        else:
            cursor = self.session.cursor()
            try:
                cursor.execute(sql)
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
            finally:
                cursor.close()

    def discover_features(self, table_name: str) -> List[FeatureDefinition]:
        parts = table_name.split('.')
        table = parts[-1]

        sql = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM {'.'.join(parts[:-1]) if len(parts) > 1 else parts[0]}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}'
        """

        features = []
        try:
            results = self._execute(sql)
            for r in results:
                col_name = r.get("COLUMN_NAME")
                data_type = r.get("DATA_TYPE", "")

                if any(t in data_type.upper() for t in ['NUMBER', 'FLOAT', 'INT', 'DOUBLE', 'DECIMAL']):
                    category = "numeric"
                elif any(t in data_type.upper() for t in ['VARCHAR', 'STRING', 'TEXT']):
                    category = "categorical"
                elif any(t in data_type.upper() for t in ['DATE', 'TIME', 'TIMESTAMP']):
                    category = "temporal"
                elif 'BOOLEAN' in data_type.upper():
                    category = "boolean"
                else:
                    category = "other"

                features.append(FeatureDefinition(
                    name=col_name,
                    expression=f'"{col_name}"',
                    data_type=data_type,
                    category=category,
                ))
        except Exception:
            pass

        return features

    def create_window_features(
        self,
        table_name: str,
        value_column: str,
        partition_column: str,
        order_column: str,
        windows: List[int] = None,
    ) -> List[FeatureDefinition]:
        windows = windows or [7, 14, 30, 60]
        features = []

        for window in windows:
            features.extend([
                FeatureDefinition(
                    name=f"{value_column}_ROLLING_AVG_{window}",
                    expression=f'AVG("{value_column}") OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}" ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)',
                    data_type="FLOAT",
                    description=f"{window}-period rolling average of {value_column}",
                    category="numeric",
                    is_derived=True,
                    source_columns=[value_column],
                ),
                FeatureDefinition(
                    name=f"{value_column}_ROLLING_SUM_{window}",
                    expression=f'SUM("{value_column}") OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}" ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)',
                    data_type="FLOAT",
                    description=f"{window}-period rolling sum of {value_column}",
                    category="numeric",
                    is_derived=True,
                    source_columns=[value_column],
                ),
                FeatureDefinition(
                    name=f"{value_column}_ROLLING_MIN_{window}",
                    expression=f'MIN("{value_column}") OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}" ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)',
                    data_type="FLOAT",
                    description=f"{window}-period rolling minimum of {value_column}",
                    category="numeric",
                    is_derived=True,
                    source_columns=[value_column],
                ),
                FeatureDefinition(
                    name=f"{value_column}_ROLLING_MAX_{window}",
                    expression=f'MAX("{value_column}") OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}" ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)',
                    data_type="FLOAT",
                    description=f"{window}-period rolling maximum of {value_column}",
                    category="numeric",
                    is_derived=True,
                    source_columns=[value_column],
                ),
                FeatureDefinition(
                    name=f"{value_column}_ROLLING_STDDEV_{window}",
                    expression=f'STDDEV("{value_column}") OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}" ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)',
                    data_type="FLOAT",
                    description=f"{window}-period rolling standard deviation of {value_column}",
                    category="numeric",
                    is_derived=True,
                    source_columns=[value_column],
                ),
            ])

        return features

    def create_lag_features(
        self,
        value_column: str,
        partition_column: str,
        order_column: str,
        lags: List[int] = None,
    ) -> List[FeatureDefinition]:
        lags = lags or [1, 7, 14, 30]
        features = []

        for lag in lags:
            features.append(FeatureDefinition(
                name=f"{value_column}_LAG_{lag}",
                expression=f'LAG("{value_column}", {lag}) OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}")',
                data_type="FLOAT",
                description=f"{value_column} lagged by {lag} periods",
                category="numeric",
                is_derived=True,
                source_columns=[value_column],
            ))

            features.append(FeatureDefinition(
                name=f"{value_column}_DIFF_{lag}",
                expression=f'"{value_column}" - LAG("{value_column}", {lag}) OVER (PARTITION BY "{partition_column}" ORDER BY "{order_column}")',
                data_type="FLOAT",
                description=f"Difference from {lag} periods ago",
                category="numeric",
                is_derived=True,
                source_columns=[value_column],
            ))

        return features

    def create_temporal_features(
        self,
        timestamp_column: str,
    ) -> List[FeatureDefinition]:
        return [
            FeatureDefinition(
                name=f"{timestamp_column}_YEAR",
                expression=f'YEAR("{timestamp_column}")',
                data_type="NUMBER",
                category="temporal",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
            FeatureDefinition(
                name=f"{timestamp_column}_MONTH",
                expression=f'MONTH("{timestamp_column}")',
                data_type="NUMBER",
                category="temporal",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
            FeatureDefinition(
                name=f"{timestamp_column}_DAY",
                expression=f'DAYOFMONTH("{timestamp_column}")',
                data_type="NUMBER",
                category="temporal",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
            FeatureDefinition(
                name=f"{timestamp_column}_DAYOFWEEK",
                expression=f'DAYOFWEEK("{timestamp_column}")',
                data_type="NUMBER",
                category="temporal",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
            FeatureDefinition(
                name=f"{timestamp_column}_HOUR",
                expression=f'HOUR("{timestamp_column}")',
                data_type="NUMBER",
                category="temporal",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
            FeatureDefinition(
                name=f"{timestamp_column}_IS_WEEKEND",
                expression=f'CASE WHEN DAYOFWEEK("{timestamp_column}") IN (0, 6) THEN 1 ELSE 0 END',
                data_type="NUMBER",
                category="boolean",
                is_derived=True,
                source_columns=[timestamp_column],
            ),
        ]

    def materialize_feature_table(
        self,
        source_table: str,
        features: List[FeatureDefinition],
        output_table: str,
        include_source_columns: bool = True,
    ) -> str:
        source_cols = "*" if include_source_columns else ""
        
        feature_exprs = [f'{f.expression} AS "{f.name}"' for f in features if f.is_derived]
        
        select_parts = []
        if source_cols:
            select_parts.append(source_cols)
        select_parts.extend(feature_exprs)

        sql = f"""
            CREATE OR REPLACE TABLE {output_table} AS
            SELECT {', '.join(select_parts)}
            FROM {source_table}
        """

        try:
            self._execute(sql)
            return output_table
        except Exception as e:
            raise RuntimeError(f"Failed to materialize feature table: {e}")

    def get_feature_stats(self, table_name: str, features: List[str]) -> Dict[str, Any]:
        stats = {}
        
        for feature in features[:20]:
            sql = f"""
                SELECT 
                    AVG("{feature}") as mean_val,
                    STDDEV("{feature}") as std_val,
                    MIN("{feature}") as min_val,
                    MAX("{feature}") as max_val,
                    APPROX_PERCENTILE("{feature}", 0.5) as median_val
                FROM {table_name}
                WHERE "{feature}" IS NOT NULL
            """
            try:
                results = self._execute(sql)
                if results:
                    stats[feature] = {
                        "mean": results[0].get("MEAN_VAL"),
                        "std": results[0].get("STD_VAL"),
                        "min": results[0].get("MIN_VAL"),
                        "max": results[0].get("MAX_VAL"),
                        "median": results[0].get("MEDIAN_VAL"),
                    }
            except Exception:
                stats[feature] = {"error": True}

        return stats
