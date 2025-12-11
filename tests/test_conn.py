import pytest
from unittest.mock import MagicMock, patch
from src import db_conn

# Test valid connection params
VALID_PARAMS = {
    "host": "localhost",
    "database": "testdb",
    "user": "user",
    "password": "pass",
    "port": 5432,
    "extra_key": "ignored"
}


def test_get_conn_context_manager_calls_connect_and_close():
    mock_conn = MagicMock()
    
    with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
        with db_conn.get_conn(VALID_PARAMS) as conn:
            assert conn == mock_conn
        
        mock_conn.close.assert_called_once()
        mock_connect.assert_called_once_with(
            host="localhost",
            database="testdb",
            user="user",
            password="pass",
            port=5432
        )


def test_get_conn_exception_closes_connection():
    mock_conn = MagicMock()
    
    with patch("psycopg2.connect", return_value=mock_conn):
        with pytest.raises(RuntimeError):
            with db_conn.get_conn(VALID_PARAMS):
                raise RuntimeError("Test exception")
        
        mock_conn.close.assert_called_once()


def test_get_conn_empty_params_connect_called_with_no_arguments():
    mock_conn = MagicMock()
    
    with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
        with db_conn.get_conn({}) as conn:
            assert conn == mock_conn
        
        mock_connect.assert_called_once_with()
        mock_conn.close.assert_called_once()
