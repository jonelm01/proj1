import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import streamlit as st
from src.main import run_etl, streamlit_run_etl, streamlit_app

@pytest.fixture
def sample_raw_df():
    return pd.DataFrame({
        "transaction_id": [1, 2],
        "product_id": [101, 102],
        "location_id": [201, 202],
        "payment_id": [301, 302],
        "total_spent": [10, 20],
        "quantity": [1, 2], 
        "transaction_date": pd.to_datetime(["2025-01-01", "2025-01-02"])
    })


@pytest.fixture
def fake_file_csv():
    class FakeFile:
        def __init__(self):
            self.type = "text/csv"
    return FakeFile()


@pytest.fixture
def fake_file_json():
    class FakeFile:
        def __init__(self):
            self.type = "application/json"
    return FakeFile()


def test_run_etl_failures(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    monkeypatch.setattr(DataExtractor, "extract", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return False
    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())

    dummy_logger = MagicMock()
    from src.load import Loader
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))

    result, *_ = run_etl("fake.csv", db_conf={})
    assert result["status"] == "failed"
    assert "pre-cleaning validation" in result["reason"]


def test_run_etl_post_clean_failure(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    monkeypatch.setattr(DataExtractor, "extract", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return True
        def clean(self, df): return (df, pd.DataFrame())
        def validate_clean_df(self, df): return False
    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())

    dummy_logger = MagicMock()
    from src.load import Loader
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))

    result, *_ = run_etl("fake.csv", db_conf={})
    assert result["status"] == "failed"
    assert "post-cleaning validation" in result["reason"]


def test_run_etl_success(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    from src.load import Loader
    monkeypatch.setattr(DataExtractor, "extract", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return True
        def clean(self, df): return (df, pd.DataFrame())
        def validate_clean_df(self, df): return True
        def normalize(self, df):
            return {
                "stg_sales": df,
                "stg_product": df[["product_id", "quantity"]].drop_duplicates(),
                "stg_location": df[["location_id"]].drop_duplicates(),
                "stg_payment_method": df[["payment_id"]].drop_duplicates()
            }
    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())
    dummy_logger = MagicMock()
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    Loader.load = MagicMock()

    result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects, df_raw, df_clean = run_etl("fake.csv", db_conf={})
    assert result["status"] == "success"
    assert not stg_sales.empty
    assert not stg_product.empty
    assert not stg_location.empty
    assert not stg_payment_method.empty


def test_streamlit_run_etl_no_file(monkeypatch):
    result, *_ = streamlit_run_etl(None, db_conf={})
    assert result["status"] == "failed"


def test_streamlit_run_etl_csv_success(monkeypatch, sample_raw_df, fake_file_csv):
    from src.extract import DataExtractor
    from src.load import Loader
    monkeypatch.setattr(DataExtractor, "extract_csv", lambda self, f: sample_raw_df)
    monkeypatch.setattr(DataExtractor, "extract_json", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return True
        def clean(self, df): return (df, pd.DataFrame())
        def validate_clean_df(self, df): return True
        def normalize(self, df): 
            return {
                "stg_sales": df,
                "stg_product": df[["product_id", "quantity"]].drop_duplicates(),
                "stg_location": df[["location_id"]].drop_duplicates(),
                "stg_payment_method": df[["payment_id"]].drop_duplicates()
            }
    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())
    dummy_logger = MagicMock()
    monkeypatch.setattr("src.extract.DataExtractor.__init__", lambda self, logger=None: setattr(self, "logger", dummy_logger))
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    Loader.load = MagicMock()

    result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects, df_raw, df_clean = streamlit_run_etl(fake_file_csv, db_conf={})
    assert result["status"] == "success"

def test_streamlit_run_etl_json_success(monkeypatch, sample_raw_df, fake_file_json):
    from src.extract import DataExtractor
    from src.load import Loader
    monkeypatch.setattr(DataExtractor, "extract_csv", lambda self, f: sample_raw_df)
    monkeypatch.setattr(DataExtractor, "extract_json", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return True
        def clean(self, df): return (df, pd.DataFrame())
        def validate_clean_df(self, df): return True
        def normalize(self, df):
            return {
                "stg_sales": df,  # includes 'quantity'
                "stg_product": pd.DataFrame({
                    "product_id": df["product_id"],
                    "product_name": [f"Product {i}" for i in range(len(df))]
                }),
                "stg_location": pd.DataFrame({
                    "location_id": df["location_id"],
                    "location_name": [f"Loc{i}" for i in range(len(df))]
                }),
                "stg_payment_method": pd.DataFrame({
                    "payment_id": df["payment_id"],
                    "payment_name": ["Card", "Cash"][:len(df)]
                })
            }

    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())
    dummy_logger = MagicMock()
    monkeypatch.setattr("src.extract.DataExtractor.__init__", lambda self, logger=None: setattr(self, "logger", dummy_logger))
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    Loader.load = MagicMock()

    result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects, df_raw, df_clean = streamlit_run_etl(fake_file_json, db_conf={})
    assert result["status"] == "success"


@patch("streamlit.file_uploader")
@patch("streamlit.button")
def test_streamlit_app_no_file(mock_button, mock_uploader):
    mock_uploader.return_value = None
    mock_button.return_value = True
    streamlit_app()  


@patch("streamlit.file_uploader")
@patch("streamlit.button")
def test_streamlit_app_with_file(mock_button, mock_uploader, monkeypatch, sample_raw_df, fake_file_csv):
    mock_uploader.return_value = fake_file_csv
    mock_button.return_value = True

    from src.main import Loader, DataExtractor, Transformer
    
    monkeypatch.setattr(DataExtractor, "extract_csv", lambda self, f: sample_raw_df)
    monkeypatch.setattr(DataExtractor, "extract_json", lambda self, f: sample_raw_df)

    class FakeTransformer:
        def __init__(self, logger=None): pass
        def validate_raw_df(self, df): return True
        def clean(self, df): return (df, pd.DataFrame())
        def validate_clean_df(self, df): return True
        def normalize(self, df):
            return {
                "stg_sales": df,  
                "stg_product": pd.DataFrame({
                    "product_id": df["product_id"],
                    "product_name": ["A", "B"][:len(df)]  
                }),
                "stg_location": pd.DataFrame({
                    "location_id": df["location_id"],
                    "location_name": ["Loc1", "Loc2"]
                }),
                "stg_payment_method": pd.DataFrame({
                    "payment_id": df["payment_id"],
                    "payment_name": ["Card", "Cash"]
                })
            }

    monkeypatch.setattr("src.main.Transformer", lambda logger=None: FakeTransformer())
    
    dummy_logger = MagicMock()
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    Loader.load = MagicMock()

    streamlit_app()
