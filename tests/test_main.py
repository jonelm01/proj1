from unittest.mock import MagicMock
from src.main import run_etl

def test_run_etl(monkeypatch, sample_raw_df):
    from src.extract import DataExtractor
    monkeypatch.setattr(DataExtractor, "extract", lambda self, p: sample_raw_df)

    from src.load import Loader
    Loader.load = MagicMock()

    from src.load import RejectsLoader
    RejectsLoader.load = MagicMock()

    result = run_etl("fake.csv", db_conf={})

    assert result["status"] == "success"
