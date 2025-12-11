import pandas as pd
from io import StringIO
import logging
from src.util import get_logger, _log_preview
from src.db_conn import get_conn
import numpy as np
import yaml

class Loader:
    def __init__(self, logger=None, conn_params=None):
        self.conn_params = conn_params or {}

        if logger:
            self.logger = logging.getLogger("Loader")
            level = getattr(logger, "level", logging.INFO)
            try:
                self.logger.setLevel(level)
            except Exception:
                self.logger.setLevel(logging.INFO)

            handlers = getattr(logger, "handlers", [])
            for h in handlers:
                try:
                    if h not in self.logger.handlers:
                        self.logger.addHandler(h)
                except Exception:
                    pass

        else:
            self.logger = get_logger(
                name="Loader",
                log_file="../logs/etl.log",
                level=logging.INFO
            )

        self.logger.info("--------------- Loader initialized ---------------")


    def _infer_pg_type(self, dtype):
        if not hasattr(dtype, "kind"):
            return "TEXT"

        if pd.api.types.is_integer_dtype(dtype): 
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype): 
            return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(dtype): 
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype): 
            return "TIMESTAMP"

        return "TEXT"

     
    def _create_table_if_not_exists(self, conn, df: pd.DataFrame, table_name: str, primary_key: str = None):
        cols = [f'"{col}" {self._infer_pg_type(dtype)}' for col, dtype in df.dtypes.items()]
        pk_sql = f", PRIMARY KEY ({primary_key})" if primary_key else ""

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(cols)}
                {pk_sql}
            );
        """

        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()

        self.logger.info(f"Created table if missing: {table_name} (PK={primary_key})")

    
    def _sanitize(self, df: pd.DataFrame) -> pd.DataFrame:
        df_safe = df.copy()

        def normalize(x):
            if pd.isna(x) or x is pd.NA or x is pd.NaT:
                return None
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()
            if isinstance(x, str) and x.strip().lower() in ("none", "nat", "nan", "unknown", ""):
                return None
            if isinstance(x, (np.integer, np.int64)):
                return int(x)
            if isinstance(x, (np.floating, np.float64)):
                return float(x)
            if isinstance(x, np.bool_):
                return bool(x)

            return x

        for col in df_safe.columns:
            df_safe[col] = df_safe[col].map(normalize).astype(object)

        self.logger.info(f"sanitize: cleaned {list(df_safe.columns)}")
        return df_safe

        
    def load(self, df: pd.DataFrame, table_name: str, conflict_cols: list[str] = None, create_if_missing=True):
        if df.empty:
            self.logger.warning(f"load: {table_name}: DataFrame empty — skipping.")
            return

        # Safe column formatting
        df = df.copy()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        df = self._sanitize(df)

        with get_conn(self.conn_params) as conn:
            if create_if_missing and conflict_cols:
                self._create_table_if_not_exists(conn, df, table_name, primary_key=conflict_cols[0])

            # UPSERT path
            if conflict_cols:
                cols = list(df.columns)
                placeholders = ", ".join(["%s"] * len(cols))
                insert_cols = ", ".join(cols)
                update_cols = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])

                upsert_sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    VALUES ({placeholders})
                    ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE
                    SET {update_cols};
                """

                cur = conn.cursor()
                cur.executemany(upsert_sql, df.itertuples(index=False, name=None))
                conn.commit()
                cur.close()

                self.logger.info(f"UPSERT: {len(df)} rows → {table_name}")
                return

            # COPY path (no PK)
            buf = StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)

            cur = conn.cursor()
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV NULL ''", buf)
            conn.commit()
            cur.close()

            self.logger.info(f"COPY: {len(df)} rows → {table_name}")

    
    def load_from_yaml(self, normalized_dict: dict, rejects_df: pd.DataFrame, source_name: str, yaml_path: str):
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)

        src_cfg = next((s for s in config["sources"] if s["name"] == source_name), None)
        if not src_cfg or "load" not in src_cfg:
            raise ValueError(f"YAML missing load rules for source '{source_name}'")

        for t in src_cfg["load"]["tables"]:
            df_key = t["df_key"]

            if df_key == "rejected":
                df = rejects_df
            else:
                df = normalized_dict.get(df_key)

            if df is None:
                self.logger.warning(f"load_from_yaml: df_key '{df_key}' not found — skipping")
                continue

            # Load each table
            self.load(
                df=df,
                table_name=t["target"],
                conflict_cols=[t["pk"]] if t.get("pk") else None
            )

        self.logger.info("--------------- All loading complete ---------------")
