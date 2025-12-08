import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from src.load import Loader, RejectsLoader


# Fake DB connection 
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


def test_infer_pg_type():
    loader = Loader(conn_params={})

    assert loader._infer_pg_type(pd.Series([1]).dtype) == "BIGINT"
    assert loader._infer_pg_type(pd.Series([1.2]).dtype) == "DOUBLE PRECISION"
    assert loader._infer_pg_type(pd.Series([True]).dtype) == "BOOLEAN"
    assert loader._infer_pg_type(pd.to_datetime(["2020-01-01"]).dtype) == "TIMESTAMP"
    assert loader._infer_pg_type(pd.Series(["x"]).dtype) == "TEXT"


def test_create_table(fake_conn):
    loader = Loader(conn_params={})

    df = pd.DataFrame({"id": [1], "name": ["x"]})
    loader._create_table_if_not_exists(fake_conn, df, "public.stg_product", primary_key="id")

    fake_conn.cursor.return_value.execute.assert_called_once()
    fake_conn.commit.assert_called_once()


def test_sanitize_rejects_timestamps_and_nans():
    loader = Loader(conn_params={})

    df = pd.DataFrame({
        "a": [pd.Timestamp("2020-01-01"), pd.NaT],
        "b": [1, None],
        "c": ["NaT", "x"]
    })

    out = loader._sanitize(df)

    assert out.loc[0, "a"].year == 2020
    assert out.loc[1, "a"] is None
    assert out.loc[0, "c"] is None


@patch("src.load.get_conn")
def test_loader_upsert(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn

    loader = Loader(conn_params={})

    df = pd.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"]
    })

    loader.load(df, "public.stg_product", conflict_cols=["id"], create_if_missing=True)

    # table creation
    assert fake_conn.cursor.return_value.execute.call_count >= 1

    # UPSERT
    fake_conn.cursor.return_value.executemany.assert_called_once()
    fake_conn.commit.assert_called()


@patch("src.load.get_conn")
def test_loader_copy(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn

    loader = Loader(conn_params={})

    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["x", "y"]
    })

    loader.load(df, "public.stg_sales", conflict_cols=None)

    assert fake_conn.cursor.return_value.copy_expert.called
    fake_conn.commit.assert_called()


def test_loader_empty_df():
    loader = Loader(conn_params={})

    loader.logger = MagicMock()  

    df = pd.DataFrame()

    out = loader.load(df, "public.stg_product", conflict_cols=["id"])
    
    assert out is None
    loader.logger.warning.assert_called()

 
def test_loader_invalid_table():
    loader = Loader(conn_params={})

    df = pd.DataFrame({"id": [1]})

    with pytest.raises(ValueError):
        loader.load(df, "public.bad_table", conflict_cols=["id"])


# ------------ REJECTS LOADER TESTS -------------

def test_rejects_sanitize():
    r = RejectsLoader(conn_params={})

    df = pd.DataFrame({
        "a": ["NaN", "x", pd.NA],
        "b": ["UNKNOWN", "y", None]
    })

    out = r._sanitize_rejects(df)

    assert out.loc[0, "a"] is None
    assert out.loc[1, "b"] == "y"
    assert out.loc[2, "a"] is None


@patch("src.load.get_conn")
def test_rejects_upsert(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn

    r = RejectsLoader(conn_params={})

    df = pd.DataFrame({
        "id": ["1", "2"],
        "reason": ["bad", "missing"]
    })

    r.load(df, conflict_cols=["id"])

    # table creation
    assert fake_conn.cursor.return_value.execute.call_count >= 1

    # UPSERT
    fake_conn.cursor.return_value.executemany.assert_called_once()
    fake_conn.commit.assert_called()


@patch("src.load.get_conn")
def test_rejects_simple_insert(mock_get_conn, fake_conn):
    mock_get_conn.return_value = fake_conn

    r = RejectsLoader(conn_params={})

    df = pd.DataFrame({
        "id": ["99"],
        "reason": ["test"]
    })

    r.load(df, conflict_cols=None)

    fake_conn.cursor.return_value.executemany.assert_called_once()
    fake_conn.commit.assert_called()


def test_rejects_empty_df():
    r = RejectsLoader(conn_params={})

    r.logger = MagicMock()

    df = pd.DataFrame()
    out = r.load(df)

    assert out is None
    r.logger.warning.assert_called()


def test_rejects_invalid_table():
    r = RejectsLoader(conn_params={})

    df = pd.DataFrame({"id": ["1"]})

    with pytest.raises(ValueError):
        r.load(df, table_name="public.not_allowed")
