import pandas as pd
import pytest
from src.validate import validate_df 

def test_validate_df_positive_sales():
    df = pd.DataFrame({"sales": [100, 200, 0, 50]})
    assert validate_df(df) is True

def test_validate_df_negative_sales():
    df = pd.DataFrame({"sales": [100, -50, 200]})
    assert validate_df(df) is False

def test_validate_df_null_sales():
    df = pd.DataFrame({"sales": [100, None, 200]})
    assert validate_df(df) is False

def test_validate_df_non_numeric():
    df = pd.DataFrame({"sales": [100, "abc", 200]})
    assert validate_df(df) is False

def test_validate_df_max_threshold():
    df = pd.DataFrame({"sales": [100, 200, 50]})
    assert validate_df(df, max_sales=150) is True

def test_validate_df_empty_dataframe():
    df = pd.DataFrame(columns=["sales"])
    assert validate_df(df) is True
