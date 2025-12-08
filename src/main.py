from src.extract import DataExtractor
from src.transform import Transformer
from src.load import Loader, RejectsLoader
from src.validate import *
import os

# python -m src.main
# TEST: pytest --cov=src --cov-report=term-missing --cov-report=html

def run_etl(input_file: str, db_conf: dict):
    # ------------------- INIT ETL ------------------
    extractor = DataExtractor()
    transformer = Transformer()
    loader = Loader(conn_params=db_conf)
    rejects_loader = RejectsLoader(conn_params=db_conf)

    # ------------------- EXTRACT -------------------
    df_raw = extractor.extract(input_file)

    # ------------------- PRE-CLEANING VALIDATION -------------------
    if not transformer.validate_raw_df(df_raw):
        return {"status": "failed", "reason": "pre-cleaning validation"}

    # ------------------- CLEAN ---------------------
    df_clean, df_rejects = transformer.clean(df_raw)

    # ------------------- POST-CLEANING VALIDATION -------------------
    if not transformer.validate_clean_df(df_clean):
        return {"status": "failed", "reason": "post-cleaning validation"}

    # ------------------- NORMALIZE -----------------
    normalized = transformer.normalize(df_clean)

    # ------------------- LOAD DIMENSION TABLES -----
    loader.load(normalized["stg_product"], "public.stg_product", conflict_cols=["product_id"])
    loader.load(normalized["stg_location"], "public.stg_location", conflict_cols=["location_id"])
    loader.load(normalized["stg_payment_method"], "public.stg_payment_method", conflict_cols=["payment_id"])

    # ------------------- LOAD REJECTS TABLE -----------
    rejects_loader.load(df_rejects)

    # ------------------- LOAD FACT TABLE -----------
    loader.load(normalized["stg_sales"], "public.stg_sales", conflict_cols=["transaction_id"])

    return {"status": "success"}


if __name__ == "__main__":
    #default conf from env
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_file = os.path.join(PROJECT_ROOT, "data", "in", "dirty_cafe_sales.csv")
    #input_file = "../data/in/dirty_cafe_sales.csv"
    db_conf = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": int(os.getenv("DB_PORT", 5432))
    }
    run_etl(input_file, db_conf)
