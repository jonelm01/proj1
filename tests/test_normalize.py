from src.transform import Transformer

def test_normalize(sample_raw_df):
    t = Transformer()
    df_clean, _ = t.clean(sample_raw_df)
    normalized = t.normalize(df_clean)

    assert "stg_sales" in normalized
    assert "stg_product" in normalized
    assert "product_id" in normalized["stg_product"].columns
