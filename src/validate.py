import pandas as pd

def validate_df(df: pd.DataFrame, max_sales: float = None) -> bool:

    if df.empty:
        print("Data validation passed: empty DataFrame.")
        return True

    if "sales" not in df.columns:
        print("Validation Error: 'sales' column not found.")
        return False

    # Coerce all values to numeric
    sales_numeric = pd.to_numeric(df['sales'], errors='coerce')

    # Check for nulls
    if sales_numeric.isnull().any():
        print("Validation Error: Null or non-numeric values found in 'sales' column.")
        return False

    # Check for negatives
    if (sales_numeric < 0).any():
        print("Validation Error: Negative sales values found.")
        return False

    # Check if sales > max
    if max_sales is not None and (sales_numeric > max_sales).any():
        print(f"Validation Warning: Sales values exceed maximum threshold of {max_sales}.")

    print("Data validation passed.")
    return True