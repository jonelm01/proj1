from src.transform import Transformer
import pandas as pd

def test_clean_fills_missing_values(sample_raw_df):
    transformer = Transformer()
    df_clean, df_rejects = transformer.clean(sample_raw_df)

    assert not df_clean.empty
    assert df_rejects.empty
    assert "Transaction ID" in df_clean.columns
