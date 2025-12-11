from src.extract import DataExtractor
from src.transform import Transformer
from src.load import Loader
from src.analytics import *
from src.util import get_logger
import os
import streamlit as st
import pandas as pd
import altair as alt
# STREAMLIT: python -m streamlit run src/main.py
#With logs: python -m streamlit run src/main.py --logger.level=info

# No interface: python -m src.main

def streamlit_run_etl( uploaded_file, db_conf, logger=None):
    if logger is None:
        logger = get_logger(name="ETL", log_file="../logs/etl.log")    

    extractor = DataExtractor(logger=logger)
    transformer = Transformer(logger=logger)
    loader = Loader(logger=logger, conn_params=db_conf)

    # Read Streamlit uploaded file into df
    if uploaded_file is None:
        st.error("No file uploaded.")
        return {"status": "failed"}, None, None, None, None, None, None, None, None

    try:
        if uploaded_file.type == "text/csv":
            df_raw = extractor.extract_csv(uploaded_file)
        elif uploaded_file.type == "application/json":
            df_raw = extractor.extract_json(uploaded_file)
    except Exception as e:
        st.error(f"Error reading file: {e}")
        return {"status": "failed"}, None, None, None, None, None, None, None, None

    if not transformer.validate_raw_df(df_raw):
        return {"status": "failed", "reason": "pre-cleaning validation"}, None, None, None, None, None, None, None, None

    df_clean, df_rejects = transformer.clean(df_raw)

    if not transformer.validate_clean_df(df_clean):
        return {"status": "failed", "reason": "post-cleaning validation"}, None, None, None, None, None, None, None, None

    normalized = transformer.normalize(df_clean)


    loader.load_from_yaml(
        normalized_dict=normalized,
        rejects_df=df_rejects,
        source_name="dirty_cafe_sales",
        yaml_path="config/sources.yml"
    )

    analytics = SalesAnalytics(
        normalized["stg_sales"],
        normalized["stg_product"],
        normalized["stg_location"],
        normalized["stg_payment_method"]
    )

    return {
        "status": "success"
    }, analytics, normalized["stg_sales"], normalized["stg_product"], normalized["stg_location"], normalized["stg_payment_method"], df_rejects, df_raw, df_clean


#Deprecated non-streamlit version
def run_etl(input_file: str, db_conf: dict, logger=None):
    if logger is None:
        logger = get_logger(name="ETL", log_file="../logs/etl.log")    
    extractor = DataExtractor(logger=logger)
    transformer = Transformer(logger=logger)
    loader = Loader(logger=logger, conn_params=db_conf)

    df_raw = extractor.extract(input_file)

    if not transformer.validate_raw_df(df_raw):
        return {"status": "failed", "reason": "pre-cleaning validation"},  None, None, None, None, None, None

    df_clean, df_rejects = transformer.clean(df_raw)

    if not transformer.validate_clean_df(df_clean):
        return {"status": "failed", "reason": "post-cleaning validation"}, None, None, None, None, None, None

    normalized = transformer.normalize(df_clean)

    loader.load_from_yaml(
        normalized_dict=normalized,
        rejects_df=df_rejects,
        source_name="dirty_cafe_sales",
        yaml_path="config/sources.yml"
    )

    analytics = SalesAnalytics(
        normalized["stg_sales"],
        normalized["stg_product"],
        normalized["stg_location"],
        normalized["stg_payment_method"]
    )

    return {
        "status": "success"
    }, analytics, normalized["stg_sales"], normalized["stg_product"], normalized["stg_location"], normalized["stg_payment_method"], df_rejects, df_raw, df_clean


def streamlit_app():
    st.set_page_config(
        page_title="Cafe Sales ETL Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    db_conf = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "port": int(os.getenv("DB_PORT", 5432))
    }
    
    st.title("Cafe Sales ETL Dashboard")
    uploaded_file = st.file_uploader("Upload CSV or JSON", type=["csv", "json"])

    if uploaded_file and st.button("Run ETL"):
        result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects, df_raw, df_clean = \
            streamlit_run_etl(uploaded_file, db_conf)

        if result["status"] != "success":
            st.error(f"ETL failed: {result.get('reason')}")
            return

        st.success("ETL completed successfully!")
        
        #Summaries
        st.header("Summary Overview")
        st.write("Raw rows:", len(df_raw), "Clean rows:", len(df_clean), "Reject rows:", len(df_rejects))
        
        total_sales = stg_sales["total_spent"].sum()
        num_transactions = len(stg_sales)
        num_products = stg_product["product_id"].nunique()
        num_locations = stg_location["location_id"].nunique()
        num_payment_methods = stg_payment_method["payment_id"].nunique()
        num_rejects = len(df_rejects)
        avg_transaction_value = total_sales / num_transactions if num_transactions > 0 else 0

        colA, colB, colC, colD = st.columns(4)
        colE, colF, colG = st.columns(3)

        with colA:
            st.metric("Total Revenue", f"${total_sales:,.2f}")
        with colB:
            st.metric("Transactions", f"{num_transactions:,}")
        with colC:
            st.metric("Avg Transaction Value", f"${avg_transaction_value:,.2f}")
        with colD:
            st.metric("Rejected Rows", num_rejects)

        with colE:
            st.metric("Unique Products", num_products)
        with colF:
            st.metric("Locations", num_locations)
        with colG:
            st.metric("Payment Methods", num_payment_methods)


        # Dataframes
        st.title("Fact Table")
        st.subheader("Sales")
        st.dataframe(stg_sales)

        st.title("Dimension Tables")
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
        
        st.title("Analytics")

        sales_df = stg_sales.copy()

        # Merge the proper naming from dimension tables so analytics can reference table with names alongside IDs
        if 'product_id' in sales_df.columns and stg_product is not None:
            product_name_col = [c for c in stg_product.columns if c != 'product_id'][0]
            sales_df = sales_df.merge(
                stg_product[['product_id', product_name_col]],
                on='product_id', how='left'
            )
        else:
            product_name_col = 'product_id'

        if 'location_id' in sales_df.columns and stg_location is not None:
            location_name_col = [c for c in stg_location.columns if c != 'location_id'][0]
            sales_df = sales_df.merge(
                stg_location[['location_id', location_name_col]],
                on='location_id', how='left'
            )
        else:
            location_name_col = 'location_id'

        if 'payment_id' in sales_df.columns and stg_payment_method is not None:
            payment_name_col = [c for c in stg_payment_method.columns if c != 'payment_id'][0]
            sales_df = sales_df.merge(
                stg_payment_method[['payment_id', payment_name_col]],
                on='payment_id', how='left'
            )
        else:
            payment_name_col = 'payment_id'
        
        #st.subheader("Sales Fact Table with Names")
        #st.dataframe(sales_df)

        # Analytics
        st.subheader("Top Products by Sales")
        col1a, col2a = st.columns(2)
        sales_by_product = sales_df.groupby(product_name_col).agg(
            total_spent=('total_spent', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()

        sales_by_product = sales_by_product.sort_values('total_spent', ascending=False)

        with col1a:
            st.dataframe(sales_by_product.head(10))
        with col2a:
            st.subheader("Sales ($) by Product")
            pie_data = sales_by_product[[product_name_col, 'total_spent']]

            fig = alt.Chart(pie_data).mark_arc().encode(
                theta='total_spent:Q',
                color=f'{product_name_col}:N',
                tooltip=[product_name_col, 'total_spent']
            )

            st.altair_chart(fig, use_container_width=True)
        
        col1b, col2b = st.columns(2)
        sales_by_location = sales_df.groupby(location_name_col).agg(
            total_spent=('total_spent', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()
        with col1b:
            st.subheader("Sales by Location")
            st.dataframe(sales_by_location)
        with col2b:
            st.subheader("Sales ($) by Location")
            st.bar_chart(sales_by_location.set_index(location_name_col)['total_spent'])

        col1c, col2c = st.columns(2)
        sales_by_payment = sales_df.groupby(payment_name_col).agg(
            total_spent=('total_spent', 'sum'),
            total_quantity=('quantity', 'sum')
        ).reset_index()
        with col1c:
            st.subheader("Sales by Payment Method")
            st.dataframe(sales_by_payment)
        with col2c:
            st.subheader("Sales($) by Payment Method")
            st.bar_chart(sales_by_payment.set_index(payment_name_col)['total_spent'])


        daily_sales = sales_df.groupby('transaction_date').agg(
            total_spent=('total_spent', 'sum'),
            num_transactions=('transaction_id', 'count')
        ).reset_index()

        col1d, col2d = st.columns(2)

        with col1d:
            st.subheader("Daily Sales")
            st.dataframe(daily_sales.sort_values('total_spent', ascending=False))

        spent_df = daily_sales[['transaction_date', 'total_spent']].copy()
        spent_df['metric'] = 'Total Spent'
        spent_df.rename(columns={'total_spent': 'value'}, inplace=True)

        transactions_df = daily_sales[['transaction_date', 'num_transactions']].copy()
        transactions_df['metric'] = 'Transactions'
        transactions_df.rename(columns={'num_transactions': 'value'}, inplace=True)
        combined_df = pd.concat([spent_df, transactions_df])

        with col2d:
            st.subheader("Daily Sales ($) and Transactions Over Time")
            chart = alt.Chart(combined_df).mark_line().encode(
                x='transaction_date:T',
                y='value:Q',
                color='metric:N',
                strokeDash=alt.condition(
                    alt.datum.metric == 'Transactions',
                    alt.value([5,5]),  # dotted
                    alt.value([1,0])   # solid
                ),
                tooltip=['transaction_date', 'value', 'metric']
            ).properties(
                width=700,
                height=400
            )

            st.altair_chart(chart, use_container_width=True)


if __name__ == "__main__":
    logger = get_logger(name="ETL", log_file="logs/etl.log")
    
    ###STREAMLIT dashboard
    streamlit_app()
    
    ###Deprecated regular run
    # PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
    # input_file = os.path.join(PROJECT_ROOT, "data", "in", "dirty_cafe_sales.csv") 
    # #input_file = "../data/in/dirty_cafe_sales.csv" 
    # db_conf = { "host": os.getenv("DB_HOST"), "database": os.getenv("DB_NAME"), "user": 
    #    os.getenv("DB_USER"), "password": os.getenv("DB_PASS"), "port": int(os.getenv("DB_PORT", 5432)) } 
    # run_etl(input_file, db_conf)
