import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from io import StringIO
import yaml
from src.load import Loader

logger = MagicMock()

@pytest.fixture
def fake_conn():
    conn = MagicMock()
    cursor = MagicMock()

    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value = cursor

    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False

    return conn


@pytest.fixture
def sample_yaml(tmp_path):
    yaml_content = {
        "sources": [
            {
                "name": "test_source",
                "load": {
                    "tables": [
                        {"df_key": "stg_product", "target": "public.stg_product", "pk": "product_id"},
                        {"df_key": "rejected", "target": "public.rejected_cafe_sales", "pk": "transaction_id"},
                    ]
                }
            }
        ]
    }
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    return yaml_file


def test_infer_pg_type():
    loader = Loader(logger, conn_params={})
    assert loader._infer_pg_type(pd.Series([1]).dtype) == "BIGINT"
    assert loader._infer_pg_type(pd.Series([1.2]).dtype) == "DOUBLE PRECISION"
    assert loader._infer_pg_type(pd.Series([True]).dtype) == "BOOLEAN"
    assert loader._infer_pg_type(pd.to_datetime(["2020-01-01"]).dtype) == "TIMESTAMP"
    assert loader._infer_pg_type(pd.Series(["x"]).dtype) == "TEXT"


def test_infer_pg_type_magicmock():
    loader = Loader(logger, conn_params={})
    fake_dtype = MagicMock()
    if hasattr(fake_dtype, "kind"):
        del fake_dtype.kind
    assert loader._infer_pg_type(fake_dtype) == "TEXT"


def test_sanitize_rejects_timestamps_and_nans():
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({
        "a": [pd.Timestamp("2020-01-01"), pd.NaT],
        "b": [1, None],
        "c": ["NaT", "x"]
    })
    out = loader._sanitize(df)
    assert out.loc[0, "a"].year == 2020
    assert pd.isna(out.loc[1, "a"])
    assert pd.isna(out.loc[0, "c"])       


def test_sanitize_various_numpy_types():
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({
        "i": [pd.Series([1], dtype="int64")[0]],
        "f": [pd.Series([1.5], dtype="float64")[0]],
        "b": [pd.Series([True], dtype="bool")[0]],
        "s": ["ok"]
    })
    out = loader._sanitize(df)
    assert isinstance(out.loc[0, "i"], int)
    assert isinstance(out.loc[0, "f"], float)
    assert isinstance(out.loc[0, "b"], bool)
    assert out.loc[0, "s"] == "ok"


def test_create_table(fake_conn):
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({"id": [1], "name": ["x"]})
    loader._create_table_if_not_exists(fake_conn, df, "public.stg_product", primary_key="id")
    fake_conn.cursor.return_value.execute.assert_called_once()
    fake_conn.commit.assert_called_once()


def test_create_table_no_pk(fake_conn):
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({"col": [1]})
    loader._create_table_if_not_exists(fake_conn, df, "public.test_table")
    fake_conn.cursor.return_value.execute.assert_called_once()


@patch("src.load.get_conn")
def test_loader_upsert(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    loader.load(df, "public.stg_product", conflict_cols=["id"], create_if_missing=True)
    assert fake_conn.cursor.return_value.execute.call_count >= 1
    fake_conn.cursor.return_value.executemany.assert_called_once()
    fake_conn.commit.assert_called()


@patch("src.load.get_conn")
def test_loader_copy(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    loader.load(df, "public.stg_sales", conflict_cols=None)
    assert fake_conn.cursor.return_value.copy_expert.called
    fake_conn.commit.assert_called()


@patch("src.load.get_conn")
def test_load_copy_empty_df(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    loader.logger = MagicMock()
    df = pd.DataFrame()
    loader.load(df, "public.test_table", conflict_cols=None)
    loader.logger.warning.assert_called_once()


@patch("src.load.get_conn")
def test_load_upsert_no_conflict_cols(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    df = pd.DataFrame({"id": [1]})
    loader.load(df, "public.stg_product", conflict_cols=[])
    assert fake_conn.cursor.return_value.copy_expert.called


def test_loader_empty_df():
    loader = Loader(logger, conn_params={})
    loader.logger = MagicMock()
    df = pd.DataFrame()
    out = loader.load(df, "public.stg_product", conflict_cols=["id"])
    assert out is None
    loader.logger.warning.assert_called()


@patch("src.load.get_conn")
def test_yaml_loader_upsert_and_rejects(mock_get_conn, fake_conn, sample_yaml):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})

    normalized_dict = {
        "stg_product": pd.DataFrame({"product_id": [1, 2], "name": ["A", "B"]})
    }
    rejects_df = pd.DataFrame({"transaction_id": [99], "reason": ["bad data"]})

    loader.load_from_yaml(normalized_dict, rejects_df, source_name="test_source", yaml_path=str(sample_yaml))
    assert fake_conn.cursor.return_value.executemany.call_count >= 2
    assert fake_conn.commit.call_count >= 2


@patch("src.load.get_conn")
def test_yaml_loader_missing_df(mock_get_conn, fake_conn, sample_yaml):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    normalized_dict = {}  # missing stg_product
    rejects_df = pd.DataFrame({"transaction_id": [1], "reason": ["bad data"]})
    loader.load_from_yaml(normalized_dict, rejects_df, source_name="test_source", yaml_path=str(sample_yaml))
    fake_conn.cursor.return_value.executemany.assert_called_once()
    fake_conn.commit.assert_called()


@patch("src.load.get_conn")
def test_yaml_loader_empty_df(mock_get_conn, fake_conn, sample_yaml):
    mock_get_conn.return_value = fake_conn
    loader = Loader(logger, conn_params={})
    loader.logger = MagicMock()
    normalized_dict = {"stg_product": pd.DataFrame()}
    rejects_df = pd.DataFrame()
    loader.load_from_yaml(normalized_dict, rejects_df, source_name="test_source", yaml_path=str(sample_yaml))
    assert loader.logger.warning.call_count >= 2


def test_load_from_yaml_missing_source(tmp_path):
    yaml_path = tmp_path / "config.yaml"
    yaml_content = {"sources": []}
    with open(yaml_path, "w") as f:
        yaml.dump(yaml_content, f)
    loader = Loader(logger, conn_params={})
    with pytest.raises(ValueError):
        loader.load_from_yaml({}, pd.DataFrame(), "missing_source", str(yaml_path))