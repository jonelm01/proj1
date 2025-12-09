import pandas as pd
import logging
from src.util import get_logger

logger = get_logger(name='Transform', log_file='../logs/etl.log', level=logging.INFO)
# Merge validators !!
class Transformer:
    def __init__(self, logger=None):
        self.logger = logger or get_logger(
            name='Transform',
            log_file='../logs/etl.log',
            level=logging.INFO
        )
        # keep transaction date as string to avoid NaT
        self.expected_schema = {
            "Transaction ID": "int64",
            "Item": "string",
            "Quantity": "int64",
            "Price Per Unit": "float",
            "Total Spent": "float",
            "Payment Method": "string",
            "Location": "string",
            "Transaction Date": "string"
        }


    # PRE-CLEANING VALIDATION
    def validate_raw_df(self, df: pd.DataFrame) -> bool:
        self.logger.info("Validation: Running pre-cleaning validation...")
        df_cols = set(df.columns)
        expected_cols = set(self.expected_schema.keys())
        missing_cols = expected_cols - df_cols
        extra_cols = df_cols - expected_cols

        if missing_cols:
            self.logger.error(f"Validation: Missing columns: {missing_cols}")
        if extra_cols:
            self.logger.warning(f"Validation: Extra columns found: {extra_cols}")
        if missing_cols:
            return False

        self.logger.info("Validation: Pre-cleaning validation passed.")
        return True


    # POST-CLEANING VALIDATION
    def validate_clean_df(self, df: pd.DataFrame) -> bool:
        self.logger.info("Validation: Running post-cleaning validation...")
        for col, expected_type in self.expected_schema.items():
            if col not in df.columns:
                continue
            actual_dtype = str(df[col].dtype)
            if expected_type == "float" and not pd.api.types.is_float_dtype(df[col]):
                self.logger.error(f"Validation: Column {col} expected float but found {actual_dtype}")
                return False
            elif expected_type == "int64" and not pd.api.types.is_integer_dtype(df[col]):
                self.logger.error(f"Validation: Column {col} expected int but found {actual_dtype}")
                return False
            elif expected_type == "string" and not pd.api.types.is_string_dtype(df[col]):
                self.logger.error(f"Validation: Column {col} expected string but found {actual_dtype}")
                return False
        self.logger.info("Validation: Post-cleaning validation passed.")
        return True


    # CLEANING
    def clean(self, df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.logger.info("Cleaning: Cleaning DataFrame...")

        df_raw.columns = [str(c).strip() for c in df_raw.columns]

        # Standardize missing values to NA
        bad_values = ["ERROR", "UNKNOWN", "NaN", "NaT", "", " ", None]
        df = df_raw.replace(bad_values, pd.NA).copy()

        # Extract numeric part from TXN_1961373 → 1961373
        df["Transaction ID"] = (
            df["Transaction ID"].astype(str).str.extract(r'(\d+)')
        )

        # Safe type conversions, nums to ints and date to str
        df["Transaction ID"] = pd.to_numeric(df["Transaction ID"], errors="coerce").astype("Int64")
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="raise").astype("Int64")
        df["Price Per Unit"] = pd.to_numeric(df["Price Per Unit"], errors="coerce").astype("float")
        df["Total Spent"] = pd.to_numeric(df["Total Spent"], errors="coerce").astype("float")
        df["Transaction Date"] = df["Transaction Date"].astype("string")
        
        
        df = self._fill_missing_values(df)


        required = ["Transaction ID", "Item", "Quantity", "Price Per Unit", "Total Spent",
                    "Payment Method", "Location"]
        #Track bad rows
        df["__missing_required__"] = df[required].isna().any(axis=1)


        df["__invalid_domain__"] = False
        #Check all ints >= 0 
        df.loc[df["Quantity"] < 0, "__invalid_domain__"] = True
        df.loc[df["Price Per Unit"] < 0, "__invalid_domain__"] = True
        df.loc[df["Total Spent"] < 0, "__invalid_domain__"] = True
        

        #df_rejects = df[
        #    df["__missing_required__"] | df["__invalid_domain__"] | df.isna().any(axis=1)
        #].copy()
        df_rejects = df[df["__missing_required__"] | df["__invalid_domain__"]].copy()
        df_clean = df.drop(df_rejects.index).copy()

        # Drop temp columns
        df_clean.drop(columns=["__missing_required__", "__invalid_domain__"], inplace=True, errors="ignore")
        df_rejects.drop(columns=["__missing_required__", "__invalid_domain__"], inplace=True, errors="ignore")


        before = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=["Transaction ID"])
        after = len(df_clean)
        self.logger.info(f"Clean: Deduplicated: removed {before - after} duplicate rows.")
        df_rejects.reset_index(drop=True, inplace=True)


        self.logger.info(f"Clean: Clean complete: {len(df_clean)} valid rows, {len(df_rejects)} rejects")
        return df_clean, df_rejects


    #  Compute missing values where possible (Total = Qty * Price)
    def _fill_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        mask_total = df["Total Spent"].isna() & df["Quantity"].notna() & df["Price Per Unit"].notna()
        df.loc[mask_total, "Total Spent"] = df.loc[mask_total, "Quantity"] * df.loc[mask_total, "Price Per Unit"]

        mask_qty = df["Quantity"].isna() & df["Total Spent"].notna() & df["Price Per Unit"].notna() & (df["Price Per Unit"] != 0)
        df.loc[mask_qty, "Quantity"] = df.loc[mask_qty, "Total Spent"] / df.loc[mask_qty, "Price Per Unit"]

        mask_price = df["Price Per Unit"].isna() & df["Total Spent"].notna() & df["Quantity"].notna() & (df["Quantity"] != 0)
        df.loc[mask_price, "Price Per Unit"] = df.loc[mask_price, "Total Spent"] / df.loc[mask_price, "Quantity"]

        self.logger.info(f"Transform: Filled {mask_total.sum()} missing totals")
        self.logger.info(f"Transform: Filled {mask_qty.sum()} missing quantities")
        self.logger.info(f"Transform: Filled {mask_price.sum()} missing prices")
        return df


    def normalize(self, df_clean: pd.DataFrame) -> dict:
        self.logger.info("Normalizing: Normalizing DataFrame...")

        df = df_clean.copy()
        df.columns = [str(c).strip() for c in df.columns]

        # Product
        stg_product = df[["Item", "Price Per Unit"]].drop_duplicates(subset=["Item"]).reset_index(drop=True)
        stg_product["product_id"] = (stg_product.index + 1).astype("int64")

        df = df.merge(stg_product[["Item", "product_id"]], on="Item", how="left")

        # Location
        stg_location = df[["Location"]].drop_duplicates().reset_index(drop=True).rename(columns={"Location": "location_type"})
        stg_location["location_id"] = (stg_location.index + 1).astype("int64")
        df = df.merge(stg_location, left_on="Location", right_on="location_type", how="left").drop(columns=["location_type"])

        # Payment
        stg_payment_method = df[["Payment Method"]].drop_duplicates().reset_index(drop=True).rename(columns={"Payment Method": "payment_method"})
        stg_payment_method["payment_id"] = (stg_payment_method.index + 1).astype("int64")
        df = df.merge(stg_payment_method, left_on="Payment Method", right_on="payment_method", how="left").drop(columns=["payment_method"])

        # Sales
        stg_sales = df[[
            "Transaction ID", "product_id", "Quantity", "Total Spent",
            "payment_id", "location_id", "Transaction Date"
        ]].rename(columns={
            "Transaction ID": "transaction_id",
            "Quantity": "quantity",
            "Total Spent": "total_spent",
            "Transaction Date": "transaction_date"
        })

        # SAFE CONV 
        for col in ["transaction_id", "product_id", "quantity", "payment_id", "location_id"]:
            stg_sales[col] = pd.to_numeric(stg_sales[col], errors="coerce").astype("Int64")

        stg_sales["total_spent"] = pd.to_numeric(stg_sales["total_spent"], errors="coerce")

        # Drop any remaining NA rows
        stg_sales = stg_sales.dropna()

        # conv to memory efficient types
        stg_sales = stg_sales.astype({
            "transaction_id": "int32",
            "product_id": "int8",
            "quantity": "int8",
            "total_spent": "float32",
            "payment_id": "int8",
            "location_id": "int8",
        })
        self.logger.info("Normalizing: Normalized DataFrame")

        return {
            "stg_sales": stg_sales,
            "stg_product": stg_product.astype({"Item": "string", "Price Per Unit": "float32", "product_id": "int8"}),
            "stg_location": stg_location.astype({"location_type": "string", "location_id": "int8"}),
            "stg_payment_method": stg_payment_method.astype({"payment_method": "string", "payment_id": "int8"}),
        }