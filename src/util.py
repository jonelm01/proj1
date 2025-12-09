import logging
from pathlib import Path
import os

def get_logger(name="ETL", log_file="logs/etl.log", level=logging.INFO):
    Path("logs").mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Dont make multiple handlers 
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
    
    return logger


def _log_preview(logger, df):
    logger.info(f"First 5 rows:\n{df.head()}")
    try:
        logger.info(f"Dataset summary:\n{df.describe()}")
    except Exception:
        logger.warning("Cannot generate summary")

