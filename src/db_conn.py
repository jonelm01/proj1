import psycopg2
import logging
from contextlib import contextmanager
from src.util import get_logger

logger = get_logger(name='Connection', log_file='../logs/etl.log', level=logging.INFO)

# Keys from .env
VALID_CONN_KEYS = {"host", "database", "user", "password", "port"}

@contextmanager
def get_conn(conn_params):
    conn = None
    safe_params = {k: v for k, v in conn_params.items() if k in VALID_CONN_KEYS}
    try:
        conn = psycopg2.connect(**safe_params)
        logger.info("DB: Postgres connection opened")
        yield conn
    finally:
        if conn:
            conn.close()
            logger.info("DB: Postgres connection closed")


