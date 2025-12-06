import pandas as pd
from io import StringIO
from src.util import get_logger, _log_preview
from src.db_conn import get_conn
import numpy as np

class Loader:
    allowed_tables = [
        "public.stg_product", "public.stg_location",
        "public.stg_payment_method", "public.stg_sales",
        "public.rejected_cafe_sales"
    ]

    def __init__(self, conn_params: dict):
        self.conn_params = conn_params
        self.logger = get_logger(name='Load', log_file='../logs/etl.log')

    # ---------------- TYPE INFERENCE ----------------
    def _infer_pg_type(self, dtype):
        if pd.api.types.is_integer_dtype(dtype): return "BIGINT"
        if pd.api.types.is_float_dtype(dtype): return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(dtype): return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMP"
        return "TEXT"

    # ---------------- TABLE CREATION ----------------
    def _create_table_if_not_exists(self, conn, df: pd.DataFrame, table_name: str, primary_key: str = None):
        cols = [f'"{col}" {self._infer_pg_type(dtype)}' for col, dtype in df.dtypes.items()]
        pk_sql = f", PRIMARY KEY ({primary_key})" if primary_key else ""
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)}{pk_sql});"
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        self.logger.info(f"Ensured table exists: {table_name} (PK={primary_key})")

    # ---------------- CONVERT PANDAS TYPES ----------------
    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = df[col].astype('Int64').astype('object')  # Keep nulls
            elif pd.api.types.is_float_dtype(df[col]):
                df[col] = df[col].astype('float')
            elif pd.api.types.is_bool_dtype(df[col]):
                df[col] = df[col].astype('object')  # Keep nulls
            else:
                # For object or datetime columns, convert NaT -> None, datetime64 -> datetime
                df[col] = df[col].apply(lambda x: x.to_pydatetime() if isinstance(x, pd.Timestamp) else (None if pd.isna(x) else x))

        #NA -> None
        df = df.where(pd.notna(df), None)
        return df
    
    def _sanitize_rejects(self, df: pd.DataFrame) -> pd.DataFrame:
        df_safe = df.copy()

        def normalize(x):
            # Nulls
            if pd.isna(x) or x is pd.NA:
                return None

            # Timestamps
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()

            # String nulls
            if isinstance(x, str) and x.strip().lower() in ("none", "nat", "nan", "unknown", ""):
                return None

            # Convert numpy types to native types
            if isinstance(x, (np.integer, np.int64)):
                return int(x)
            if isinstance(x, (np.floating, np.float64)):
                return float(x)
            if isinstance(x, (np.bool_)):
                return bool(x)

            return x

        for col in df_safe.columns:
            normalized_values = [normalize(x) for x in df_safe[col]]
            df_safe[col] = pd.Series(normalized_values, dtype=object)

        return df_safe


    def df_to_safe_python(df: pd.DataFrame) -> pd.DataFrame:
        df_safe = df.copy()
        for col in df_safe.columns:
            if pd.api.types.is_datetime64_any_dtype(df_safe[col]):
                df_safe[col] = df_safe[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
            else:
                df_safe[col] = df_safe[col].apply(lambda x: None if pd.isna(x) else x)
        return df_safe


    # ---------------- LOAD DATAFRAME ----------------
    def load(self, df: pd.DataFrame, table_name: str, conflict_cols: list[str] = None, create_if_missing=True):
        if df.empty:
            self.logger.warning(f"{table_name}: DataFrame empty — nothing to load.")
            return

        df = df.copy()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Convert pandas types to safe Python types
        df = self._sanitize_rejects(df)

        if table_name not in self.allowed_tables:
            raise ValueError(f"Table {table_name} not allowed")

        with get_conn(self.conn_params) as conn:

            if create_if_missing and conflict_cols:
                self._create_table_if_not_exists(conn, df, table_name, primary_key=conflict_cols[0])

            if conflict_cols:
                cols = df.columns.tolist()
                placeholders = ', '.join(['%s'] * len(cols))
                insert_cols = ', '.join(cols)
                update_cols = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])

                insert_sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE
                    SET {update_cols};
                """
                cur = conn.cursor()
                cur.executemany(insert_sql, df.itertuples(index=False, name=None))
                conn.commit()
                cur.close()
                self.logger.info(f"UPSERTED {len(df)} rows into {table_name}")

            else:
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                cur = conn.cursor()
                cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL ''", buffer)
                conn.commit()
                cur.close()
                self.logger.info(f"Loaded {len(df)} rows → {table_name}")

            _log_preview(self.logger, df)
            


class RejectsLoader:
    allowed_tables = ["public.rejected_cafe_sales"]

    def __init__(self, conn_params: dict):
        self.conn_params = conn_params
        self.logger = get_logger(name='RejectsLoad', log_file='../logs/etl.log')

    def _sanitize_rejects(self, df: pd.DataFrame) -> pd.DataFrame:
        df_safe = df.copy()

        def normalize(x):
            # Nulls
            if pd.isna(x) or x is pd.NA:
                return None

            # Timestamps
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()

            # Strings representing nulls
            if isinstance(x, str) and x.strip().lower() in ("none", "nat", "nan", "unknown", ""):
                return None

            # Convert numpy types to native Python types
            if isinstance(x, (np.integer, np.int64)):
                return int(x)
            if isinstance(x, (np.floating, np.float64)):
                return float(x)
            if isinstance(x, (np.bool_)):
                return bool(x)

            return x

        for col in df_safe.columns:
            normalized_values = [normalize(x) for x in df_safe[col]]
            df_safe[col] = pd.Series(normalized_values, dtype=object)

        return df_safe


    def _create_table_if_not_exists(self, conn, df: pd.DataFrame, table_name: str, primary_key: str = None):
        cols_def = [f'"{col}" TEXT' for col in df.columns]
        pk_sql = f", PRIMARY KEY ({primary_key})" if primary_key else ""
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols_def)}{pk_sql});"
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        self.logger.info(f"Ensured rejected table exists: {table_name} (PK={primary_key})")

    def load(self, df: pd.DataFrame, table_name: str = "public.rejected_cafe_sales", conflict_cols: list[str] = None, create_if_missing=True):
        
        if df.empty:
            self.logger.warning("Rejects DataFrame empty — nothing to load.")
            return

        if table_name not in self.allowed_tables:
            raise ValueError(f"Table {table_name} not allowed")

        df_safe = self._sanitize_rejects(df)
        df_safe.columns = [c.lower().replace(" ", "_") for c in df_safe.columns]

        with get_conn(self.conn_params) as conn:
            cur = conn.cursor()

            # create table if missing
            if create_if_missing:
                self._create_table_if_not_exists(conn, df_safe, table_name, primary_key=conflict_cols[0] if conflict_cols else None)

            if conflict_cols:
                # Upsert
                cols = df_safe.columns.tolist()
                placeholders = ', '.join(['%s'] * len(cols))
                insert_cols = ', '.join(cols)
                update_cols = ', '.join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])

                insert_sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE
                    SET {update_cols};
                """
                tuples_to_insert = [tuple(row) for row in df_safe.to_numpy()]
                cur.executemany(insert_sql, tuples_to_insert)
                self.logger.info(f"UPSERTED {len(df_safe)} rejected rows → {table_name}")
            else:
                # Normal insert
                placeholders = ', '.join(['%s'] * len(df_safe.columns))
                insert_cols = ', '.join(df_safe.columns)
                insert_sql = f"INSERT INTO {table_name} ({insert_cols}) VALUES ({placeholders});"
                tuples_to_insert = [tuple(row) for row in df_safe.to_numpy()]
                cur.executemany(insert_sql, tuples_to_insert)
                self.logger.info(f"Loaded {len(df_safe)} rejected rows → {table_name}")

            conn.commit()
            cur.close()
            _log_preview(self.logger, df_safe)
