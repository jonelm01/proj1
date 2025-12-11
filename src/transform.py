import pandas as pd
import logging
from src.util import get_logger
import yaml

logger = get_logger(name='Transform', log_file='../logs/etl.log', level=logging.INFO)

class Transformer:
    def __init__(self, schema_path: str = "config/sources.yml", source_name: str = "dirty_cafe_sales", logger=None):
        if logger:
            self.logger = logging.getLogger("Transform")
            self.logger.setLevel(logger.level)
            for h in logger.handlers:
                if h not in self.logger.handlers:
                    self.logger.addHandler(h)
        else:
            self.logger = get_logger(
                name='Transform',
                log_file='../logs/etl.log',
                level=logging.INFO
            )
        self.logger.info("------------------------ Transformer initialized -----------------------")

        self.source_config = self._load_source_config(schema_path, source_name)
        self.expected_schema = self._load_schema(schema_path, source_name)
        self.expected_cleaning = self._load_cleaning(schema_path, source_name)


    def _load_schema(self, path: str, source_name: str) -> dict:
        self.logger.info(f"_load_schema: Loading schema for '{source_name}' from {path}")
        try:
            with open(path, "r") as f:
                config = yaml.safe_load(f)

            for source in config.get("sources", []):
                if source.get("name") == source_name:
                    return source.get("schema", {})

            self.logger.error(f"_load_schema: No schema found for source '{source_name}'")
            raise ValueError(f"_load_schema: No schema found for source '{source_name}'")
        except FileNotFoundError:
            self.logger.error(f"_load_schema: Schema file not found at {path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"_load_schema: Error parsing YAML file: {e}")
            raise
        
        
    def _load_cleaning(self, path: str, source_name: str) -> dict:
        self.logger.info(f"_load_cleaning: Loading cleaning rules for '{source_name}' from {path}")
        try:
            with open(path, "r") as f:
                config = yaml.safe_load(f)

            for source in config.get("sources", []):
                if source.get("name") == source_name:
                    return source.get("cleaning", {})

            self.logger.error(f"_load_cleaning: No cleaning rules found for source '{source_name}'")
            raise ValueError(f"_load_cleaning: No cleaning rules found for source '{source_name}'")
        except FileNotFoundError:
            self.logger.error(f"_load_cleaning: Cleaning rules file not found at {path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"_load_cleaning: Error parsing YAML file: {e}")
            raise
    
    
    def _load_source_config(self, path: str, source_name: str) -> dict:
        self.logger.info(f"_load_source_config: Loading full source config for '{source_name}' from {path}")
        try:
            with open(path, "r") as f:
                config = yaml.safe_load(f)

            for source in config.get("sources", []):
                if source.get("name") == source_name:
                    return source  

            raise ValueError(f"_load_source_config: No config found for source '{source_name}'")

        except FileNotFoundError:
            self.logger.error(f"_load_source_config: Config file not found at {path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"_load_source_config: Error parsing YAML: {e}")
            raise


    # Pre-clean validation
    def validate_raw_df(self, df: pd.DataFrame) -> bool:
        self.logger.info("validate_raw_df: Running pre-cleaning validation...")
        df_cols = set(df.columns)
        expected_cols = set(self.expected_schema.keys())
        missing_cols = expected_cols - df_cols
        extra_cols = df_cols - expected_cols

        if missing_cols:
            self.logger.error(f"validate_raw_df: Missing columns: {missing_cols}")
        if extra_cols:
            self.logger.warning(f"validate_raw_df: Extra columns found: {extra_cols}")
        if missing_cols:
            return False

        self.logger.info("validate_raw_df: Pre-cleaning validation passed.")
        return True


    #  Compute missing values where possible (ex. Total = Qty * Price)
    def _fill_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        mask_total = df["Total Spent"].isna() & df["Quantity"].notna() & df["Price Per Unit"].notna()
        df.loc[mask_total, "Total Spent"] = df.loc[mask_total, "Quantity"] * df.loc[mask_total, "Price Per Unit"]

        mask_qty = df["Quantity"].isna() & df["Total Spent"].notna() & df["Price Per Unit"].notna() & (df["Price Per Unit"] != 0)
        df.loc[mask_qty, "Quantity"] = df.loc[mask_qty, "Total Spent"] / df.loc[mask_qty, "Price Per Unit"]

        mask_price = df["Price Per Unit"].isna() & df["Total Spent"].notna() & df["Quantity"].notna() & (df["Quantity"] != 0)
        df.loc[mask_price, "Price Per Unit"] = df.loc[mask_price, "Total Spent"] / df.loc[mask_price, "Quantity"]

        self.logger.info(f"_fill_missing_values: Filled {mask_total.sum()} missing totals")
        self.logger.info(f"_fill_missing_values: Filled {mask_qty.sum()} missing quantities")
        self.logger.info(f"_fill_missing_values: Filled {mask_price.sum()} missing prices")
        return df


    # Clean, standarduze bad values, trim ID, type conversions, compute empties if possible
    def clean(self, df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.logger.info("clean: Cleaning DataFrame...")

        # Clean column names
        df_raw.columns = [str(c).strip() for c in df_raw.columns]

        # Standardize missing vals
        bad_values = set(self.expected_cleaning.get("missing_values", []))
        df = df_raw.replace(bad_values, pd.NA).copy()

        # Apply transformations (ID trim, numeric, to_string)
        df = self._apply_transformations(df)

        # Compute missing values where possible
        df = self._fill_missing_values(df)

        # Track missing req fields for rejection
        df = self._mark_missing_required(df)

        # Domain check on cols
        df = self._apply_domain_rules(df)

        # Split clean and rejects
        df_rejects = df[df["__missing_required__"] | df["__invalid_domain__"]].copy()
        df_clean = df.drop(df_rejects.index).copy()

        # Drop temp marked columns
        df_clean.drop(columns=["__missing_required__", "__invalid_domain__"], inplace=True, errors="ignore")
        df_rejects.drop(columns=["__missing_required__", "__invalid_domain__"], inplace=True, errors="ignore")

        # Deduplicate using PK from YAML
        pk = self.expected_schema.get("pk", ["Transaction ID"])
        if isinstance(pk, str):
            pk = [pk]

        before = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=pk)
        after = len(df_clean)
        self.logger.info(f"clean: Deduplicated: removed {before - after} duplicate rows.")

        df_rejects.reset_index(drop=True, inplace=True)

        self.logger.info(f"clean: Complete: {len(df_clean)} valid rows, {len(df_rejects)} rejects")
        return df_clean, df_rejects


    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        rules = self.expected_cleaning.get("transformations", [])

        # Apply each transformation rule from YAML
        for rule in rules:
            col = rule.get("column")
            if col not in df.columns:
                self.logger.warning(f"_apply_transformations: Column '{col}' not found. Skipping.")
                continue

            # Regex application
            if "regex_extract" in rule:
                pattern = rule["regex_extract"]
                self.logger.info(f"_apply_transformations: regex_extract on '{col}' using '{pattern}'")
                df[col] = df[col].astype(str).str.extract(pattern)

            # Numeric conversions
            if "numeric" in rule:
                cast_type = rule["numeric"]
                self.logger.info(f"_apply_transformations: casting '{col}' to {cast_type}")

                if cast_type == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif cast_type == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float")

            # String conversion
            if rule.get("to_string"):
                df[col] = df[col].astype("string")

        return df
    
    
    def _mark_missing_required(self, df: pd.DataFrame) -> pd.DataFrame:
        # Identify required fields from YAML
        required = self.expected_cleaning.get("required_fields", [])

        df["__missing_required__"] = df[required].isna().any(axis=1)
        return df


    def _apply_domain_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        # Apply domain rules from YAML
        rules = self.expected_cleaning.get("domain_rules", [])
        df["__invalid_domain__"] = False

        for rule in rules:
            col = rule.get("column")
            expr = rule.get("must_be")  # Check for numbers exceeding bounds set by rules

            if col not in df.columns:
                self.logger.warning(f"apply_domain_rules: Column '{col}' not found, skipping.")
                continue

            condition = f"df['{col}'] {expr}"

            try:
                invalid_mask = ~eval(condition)
            except Exception as e:
                self.logger.error(f"apply_domain_rules: Invalid domain rule '{condition}': {e}")
                continue

            df.loc[invalid_mask, "__invalid_domain__"] = True

        return df


    #Post-clean validation
    def validate_clean_df(self, df: pd.DataFrame) -> bool:
        self.logger.info("validation_clean_df: Running post-cleaning validation...")
        for col, expected_type in self.expected_schema.items():
            if col not in df.columns:
                continue
            if col in self.expected_cleaning.get("required_fields", []) and df[col].isna().any():
                self.logger.error(f"validation_clean_df: Column {col} has missing values")
                return False
            actual_dtype = str(df[col].dtype)
            if expected_type == "float" and not pd.api.types.is_float_dtype(df[col]):
                self.logger.error(f"validation_clean_df: Column {col} expected float but found {actual_dtype}")
                return False
            elif expected_type == "int64" and not pd.api.types.is_integer_dtype(df[col]):
                self.logger.error(f"validation_clean_df: Column {col} expected int but found {actual_dtype}")
                return False
            elif expected_type == "string" and not pd.api.types.is_string_dtype(df[col]):
                self.logger.error(f"validation_clean_df: Column {col} expected string but found {actual_dtype}")
                return False
        self.logger.info("validation_clean_df: Post-cleaning validation passed.")

        return True
    

    def normalize(self, df_clean: pd.DataFrame) -> dict:
        self.logger.info("normalize: Normalizing DataFrame...")

        df = df_clean.copy()
        df.columns = [str(c).strip() for c in df.columns]

        normalized_outputs = {}

        # Get normalize config from YAML
        norm_cfg = self.source_config.get("normalize", {})
        dimensions_cfg = norm_cfg.get("dimensions", [])
        fact_cfg = norm_cfg.get("fact", {})

        # Normalize, apply renaming column names
        for dim_cfg in dimensions_cfg:
            rename_map = dim_cfg.get("rename", {})
            if rename_map:
                df = df.rename(columns=rename_map)

        # Process each dimension
        for dim_cfg in dimensions_cfg:
            dim_name = dim_cfg["name"]
            # Columns from the raw df to include in table
            source_cols = dim_cfg["source_columns"]
            rename_map = dim_cfg.get("rename", {})
            # Name of surrogate key column to create for table
            surrogate_key = dim_cfg["surrogate_key"]
            # Columns to track duplicates
            dedupe_on = dim_cfg.get("dedupe_on", source_cols)

            # Apply rename to dedupe columns
            dedupe_on_renamed = [rename_map.get(c, c) for c in dedupe_on]

            # Extract only the relevant columns for dim table
            dim_df = df[[rename_map.get(c, c) for c in source_cols]].copy()

            # Drop duplicates and reset index
            dim_df = dim_df.drop_duplicates(subset=dedupe_on_renamed).reset_index(drop=True)

            # Add surrogate key
            dim_df[surrogate_key] = (dim_df.index + 1).astype(dim_cfg.get("dtype", "int32"))

            # Merge surrogate key back into main df
            left_keys = [rename_map.get(c, c) for c in source_cols]  # keys in source df
            right_keys = [rename_map.get(c, c) for c in source_cols]  # keys in dimension df
            df = df.merge(dim_df[[*right_keys, surrogate_key]], left_on=left_keys, right_on=right_keys, how="left")

            # Save dimension table in dict 
            normalized_outputs[f"stg_{dim_name}"] = dim_df

        # Process fact table from source df and dimension tables
        fact_columns_map = fact_cfg.get("columns", {})
        surrogate_keys = fact_cfg.get("surrogate_keys", [])
        safe_numeric = fact_cfg.get("safe_numeric", [])
        float_columns = fact_cfg.get("float_columns", [])
        final_dtypes = fact_cfg.get("final_dtypes", {})

        # Combine source fact cols and surrogate keys
        available_cols = [c for c in list(fact_columns_map.keys()) + surrogate_keys if c in df.columns]
        stg_fact = df[available_cols].copy()
        stg_fact = stg_fact.rename(columns=fact_columns_map)

        # Safe numeric conversions
        for col in safe_numeric:
            if col in stg_fact.columns:
                stg_fact[col] = pd.to_numeric(stg_fact[col], errors="coerce").astype("Int64")

        for col in float_columns:
            if col in stg_fact.columns:
                stg_fact[col] = pd.to_numeric(stg_fact[col], errors="coerce")

        # Drop rows with NA in any fact cols
        stg_fact = stg_fact.dropna()

        # Convert to final data types
        stg_fact = stg_fact.astype({k: v for k, v in final_dtypes.items() if k in stg_fact.columns})

        # Save fact table
        normalized_outputs[fact_cfg["name"]] = stg_fact

        table_names = list(normalized_outputs.keys())
        self.logger.info(f"normalize: Normalization complete. Tables created: {table_names}")
        self.logger.info("------------------------ Transformations Complete -----------------------")

        return normalized_outputs