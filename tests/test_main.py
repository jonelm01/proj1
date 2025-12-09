from unittest.mock import MagicMock
from src.main import run_etl, streamlit_run_etl

def test_run_etl(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    from src.load import Loader, RejectsLoader

    # Mock extraction
    monkeypatch.setattr(DataExtractor, "extract", lambda self, p: sample_raw_df)

    # Mock logger
    dummy_logger = MagicMock()

    # Patch Loader and RejectsLoader to always get dummy_logger
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    monkeypatch.setattr("src.load.RejectsLoader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))

    # Mock load methods to avoid DB writes
    Loader.load = MagicMock()
    RejectsLoader.load = MagicMock()

    result, _, _ = run_etl("fake.csv", db_conf={})

    assert result["status"] == "success"


def test_streamlit_run_etl(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    from src.load import Loader, RejectsLoader

    # -----------------------------
    # Mock extraction
    # -----------------------------
    monkeypatch.setattr(DataExtractor, "extract_csv", lambda self, file: sample_raw_df)
    monkeypatch.setattr(DataExtractor, "extract_json", lambda self, file: sample_raw_df)

    # -----------------------------
    # Mock logger
    # -----------------------------
    dummy_logger = MagicMock()
    monkeypatch.setattr("src.extract.DataExtractor.__init__", lambda self, logger=None: setattr(self, "logger", dummy_logger))
    #monkeypatch.setattr("src.transform.Transformer.__init__", lambda self, logger=None: setattr(self, "logger", dummy_logger))
    monkeypatch.setattr("src.load.Loader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))
    monkeypatch.setattr("src.load.RejectsLoader.__init__", lambda self, conn_params, logger=None: setattr(self, "logger", dummy_logger))

    # -----------------------------
    # Mock DB loads
    # -----------------------------
    Loader.load = MagicMock()
    RejectsLoader.load = MagicMock()

    # -----------------------------
    # Fake uploaded file
    # -----------------------------
    class FakeUploadedFile:
        def __init__(self, type_):
            self.type = type_

    fake_csv_file = FakeUploadedFile(type_="text/csv")
    fake_db_conf = {}

    # -----------------------------
    # Call the streamlit_run_etl function
    # -----------------------------
    result, analytics, stg_sales, stg_product, stg_location, stg_payment_method, df_rejects = streamlit_run_etl(
        fake_csv_file, fake_db_conf
    )

    # -----------------------------
    # Assertions
    # -----------------------------
    assert result["status"] == "success"
    assert not stg_sales.empty
    assert not stg_product.empty
    assert not stg_location.empty
    assert not stg_payment_method.empty