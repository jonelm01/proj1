import pandas as pd
import pytest
from src.transform import Transformer

@pytest.fixture
def transformer():
    return Transformer(logger=None)


def test_validate_raw_df_pass(transformer):
    df = pd.DataFrame({
        "Transaction ID": [1],
        "Item": ["A"],
        "Quantity": [2],
        "Price Per Unit": [1.5],
        "Total Spent": [3.0],
        "Payment Method": ["Cash"],
        "Location": ["NY"],
        "Transaction Date": ["2024-01-01"]
    })

    assert transformer.validate_raw_df(df) is True


def test_validate_raw_df_missing_cols(transformer):
    df = pd.DataFrame({
        "Transaction ID": [1],
        "Item": ["A"]
        # missing other req columns
    })

    assert transformer.validate_raw_df(df) is False


def test_validate_raw_df_extra_cols(transformer):
    df = pd.DataFrame({
        "Transaction ID": [1],
        "Item": ["A"],
        "Quantity": [2],
        "Price Per Unit": [1.5],
        "Total Spent": [3.0],
        "Payment Method": ["Cash"],
        "Location": ["NY"],
        "Transaction Date": ["2024-01-01"],
        "ExtraColumn": [999]  
    })

    assert transformer.validate_raw_df(df) is True


def test_validate_clean_df_valid_types(transformer):
    df = pd.DataFrame({
        "Transaction ID": pd.Series([1], dtype="int64"),
        "Item": pd.Series(["A"], dtype="string"),
        "Quantity": pd.Series([2], dtype="int64"),
        "Price Per Unit": pd.Series([1.5], dtype="float64"),
        "Total Spent": pd.Series([3.0], dtype="float64"),
        "Payment Method": pd.Series(["Cash"], dtype="string"),
        "Location": pd.Series(["NY"], dtype="string"),
        "Transaction Date": pd.Series(["2024-01-01"], dtype="string")
    })

    assert transformer.validate_clean_df(df) is True


def test_validate_clean_df_wrong_type_int(transformer):
    df = pd.DataFrame({
        "Transaction ID": pd.Series(["not an int"], dtype="string"),  # should be int64
        "Item": pd.Series(["A"], dtype="string")
    })

    assert transformer.validate_clean_df(df) is False


def test_validate_clean_df_wrong_type_float(transformer):
    df = pd.DataFrame({
        "Transaction ID": pd.Series([1], dtype="int64"),
        "Item": pd.Series(["A"], dtype="string"),
        "Quantity": pd.Series([2], dtype="int64"),
        "Price Per Unit": pd.Series(["wrong"], dtype="string"),  # expected float
    })

    assert transformer.validate_clean_df(df) is False


def test_validate_clean_df_missing_optional_cols(transformer):
    df = pd.DataFrame({
        "Transaction ID": pd.Series([1], dtype="int64"),
        "Item": pd.Series(["A"], dtype="string")
    })

    assert transformer.validate_clean_df(df) is True
