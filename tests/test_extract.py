from src.extract import DataExtractor
import pandas as pd
import os

def test_extract_csv(tmp_path):
    # create temp csv file
    p = tmp_path / "test.csv"
    p.write_text("a,b\n1,2")

    extractor = DataExtractor()
    df = extractor.extract(str(p))

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]
