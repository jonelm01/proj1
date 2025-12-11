import pandas as pd
import pytest
from src.transform import Transformer

@pytest.fixture
def sample_valid_df():
    return pd.DataFrame({
        "Transaction ID": ["TXN_1", "TXN_2", "TXN_3"],
        "Item": ["Coffee", "Tea", "Latte"],
        "Quantity": [1, 2, None],  # test fill
        "Price Per Unit": [3.5, 2.5, 4.0],
        "Total Spent": [None, 5.0, 4.0],  # test fill
        "Payment Method": ["Cash", "Card", "Cash"],
        "Location": ["Store1", "Store2", "Store1"],
        "Transaction Date": ["2025-12-01", "2025-12-01", "2025-12-01"]
    })


def test_clean_fills_total_spent(sample_valid_df):
    transformer = Transformer()
    df_clean, df_rejects = transformer.clean(sample_valid_df)

    filled_total = df_clean.loc[df_clean["Transaction ID"] == 1, "Total Spent"].iloc[0]
    assert filled_total == 3.5
    assert df_rejects.empty


def test_clean_fills_quantity(sample_valid_df):
    transformer = Transformer()
    df_clean, df_rejects = transformer.clean(sample_valid_df)

    filled_qty = df_clean.loc[df_clean["Transaction ID"] == 3, "Quantity"].iloc[0]
    assert filled_qty == 1
    assert df_rejects.empty


def test_clean_deduplicates(sample_valid_df):
    df_dupes = pd.concat([sample_valid_df, sample_valid_df], ignore_index=True)
    transformer = Transformer()
    df_clean, df_rejects = transformer.clean(df_dupes)

    assert df_clean["Transaction ID"].nunique() == 3


def test_clean_type_conversion(sample_valid_df):
    transformer = Transformer()
    df_clean, _ = transformer.clean(sample_valid_df)

    assert pd.api.types.is_integer_dtype(df_clean["Transaction ID"])
    assert pd.api.types.is_integer_dtype(df_clean["Quantity"])
    assert pd.api.types.is_float_dtype(df_clean["Price Per Unit"])
    assert pd.api.types.is_float_dtype(df_clean["Total Spent"])
    assert pd.api.types.is_string_dtype(df_clean["Item"])


def test_normalize_structure(sample_valid_df):
    transformer = Transformer()
    df_clean, _ = transformer.clean(sample_valid_df)
    normalized = transformer.normalize(df_clean)

    assert "stg_sales" in normalized
    assert "stg_product" in normalized
    assert "stg_location" in normalized
    assert "stg_payment_method" in normalized

    sales_cols = ["transaction_id", "product_id", "quantity", "total_spent", "payment_id", "location_id", "transaction_date"]
    for col in sales_cols:
        assert col in normalized["stg_sales"].columns
