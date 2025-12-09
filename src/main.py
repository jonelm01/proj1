from src.extract import DataExtractor
from src.transform import Transformer
from src.load import Loader, RejectsLoader
from src.validate import *
from src.analytics import *
from src.util import get_logger
import os
import streamlit as st
import pandas as pd



# STREAMLIT: python -m streamlit run src/main.py
#With logs: python -m streamlit run src/main.py --logger.level=info
# REGULAR: python -m src.main
def run_etl(input_file: str, db_conf: dict):
    logger = get_logger(name="ETL", log_file="../logs/etl.log")
    # ------------------- INIT ETL ------------------
    extractor = DataExtractor(logger=logger)
    transformer = Transformer(logger=logger)
    loader = Loader(logger=logger, conn_params=db_conf)
    rejects_loader = RejectsLoader(logger=logger, conn_params=db_conf)

    # ------------------- EXTRACT -------------------
    df_raw = extractor.extract(input_file)

    # ------------------- PRE-CLEANING VALIDATION -------------------
    if not transformer.validate_raw_df(df_raw):
        return {"status": "failed", "reason": "pre-cleaning validation"}, None, None

    # ------------------- CLEAN ---------------------
    df_clean, df_rejects = transformer.clean(df_raw)

    # ------------------- POST-CLEANING VALIDATION -------------------
    if not transformer.validate_clean_df(df_clean):
        return {"status": "failed", "reason": "post-cleaning validation"}, None, None

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

    analytics = SalesAnalytics(
        normalized["stg_sales"],
        normalized["stg_product"],
        normalized["stg_location"],
        normalized["stg_payment_method"]
    )

    return {"status": "success"}, analytics, df_rejects

def streamlit_run_etl(uploaded_file, db_conf, logger=None):
    if logger is None:
        logger = get_logger(name="ETL", log_file="../logs/etl.log")    
    extractor = DataExtractor(logger=logger)
    transformer = Transformer(logger=logger)
    loader = Loader(logger=logger,conn_params=db_conf)
    rejects_loader = RejectsLoader(logger=logger,conn_params=db_conf)

    # Read uploaded file into a DataFrame
    if uploaded_file is None:
        st.error("No file uploaded.")
        return {"status": "failed"}, None, None

    try:
        if uploaded_file.type == "text/csv":
            df_raw = extractor.extract_csv(uploaded_file)
        elif uploaded_file.type == "application/json":
            df_raw = extractor.extract_json(uploaded_file)
    except Exception as e:
        st.error(f"Error reading file: {e}")
        return {"status": "failed"}, None, None

    

    if not transformer.validate_raw_df(df_raw):
        return {"status": "failed", "reason": "pre-cleaning validation"}, None, None

    df_clean, df_rejects = transformer.clean(df_raw)

    if not transformer.validate_clean_df(df_clean):
        return {"status": "failed", "reason": "post-cleaning validation"}, None, None

    normalized = transformer.normalize(df_clean)

    loader.load(normalized["stg_product"], "public.stg_product", conflict_cols=["product_id"])
    loader.load(normalized["stg_location"], "public.stg_location", conflict_cols=["location_id"])
    loader.load(normalized["stg_payment_method"], "public.stg_payment_method", conflict_cols=["payment_id"])
    rejects_loader.load(df_rejects)
    loader.load(normalized["stg_sales"], "public.stg_sales", conflict_cols=["transaction_id"])

    analytics = SalesAnalytics(
        normalized["stg_sales"],
        normalized["stg_product"],
        normalized["stg_location"],
        normalized["stg_payment_method"]
    )

    return {"status": "success"}, analytics, normalized["stg_sales"], normalized["stg_product"], normalized["stg_location"], normalized["stg_payment_method"], df_rejects

    


def streamlit_app():
    st.title("Cafe Sales ETL Dashboard")

    uploaded_file = st.file_uploader("Upload CSV or JSON", type=["csv", "json"])

    db_conf = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": int(os.getenv("DB_PORT", 5432))
    }

    if uploaded_file and st.button("Run ETL"):
        result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects = streamlit_run_etl(uploaded_file, db_conf)

        if result["status"] == "success":
            st.success("ETL completed successfully!")

            st.subheader("Sales")
            st.dataframe(stg_sales)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Products")
                st.dataframe(stg_product)
            with col2:
                st.subheader("Locations")
                st.dataframe(stg_location)
            with col3:
                st.subheader("Payment Methods")
                st.dataframe(stg_payment_method)
            
            st.subheader("Rejected Rows")
            st.dataframe(df_rejects)

            st.subheader("Top Products by Sales")
            col1a, col2a = st.columns(2)
            with col1a:
                st.dataframe(analytics.sales_by_product().head(10))
            with col2a:
                st.bar_chart(analytics.sales_by_product().set_index('Item')['total_spent'])
            
            st.subheader("Sales by Location")
            col1b, col2b = st.columns(2)
            with col1b:
                st.dataframe(analytics.sales_by_location().set_index("location_type")["total_spent"])

            with col2b:
                st.bar_chart(analytics.sales_by_location().set_index("location_type")["total_spent"])
                
            
            st.subheader("Daily Sales")
            col1c, col2c = st.columns(2)
            with col1c:
                st.dataframe(analytics.daily_sales().sort_values(by="total_spent", ascending=False).set_index("transaction_date")["total_spent"])    
            with col2c:
                st.line_chart(analytics.daily_sales().set_index('transaction_date')['total_spent'])

        else:
            st.error(f"ETL failed: {result.get('reason')}")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    logger = get_logger(name="ETL", log_file="logs/etl.log")

    ###STREAMLIT dashboard
    streamlit_app()
    
    ###Regular run
    # PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    # input_file = os.path.join(PROJECT_ROOT, "data", "in", "dirty_cafe_sales.csv") 
    # #input_file = "../data/in/dirty_cafe_sales.csv" 
    # db_conf = { "host": os.getenv("DB_HOST"), "database": os.getenv("DB_NAME"), "user": 
    #    os.getenv("DB_USER"), "password": os.getenv("DB_PASS"), "port": int(os.getenv("DB_PORT", 5432)) } 
    # run_etl(input_file, db_conf)

