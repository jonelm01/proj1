import pandas as pd
import logging
import json
from pathlib import path
from src.util import get_logger, _log_preview
import os


class DataExtractor:
    def __init__(self, config, logger=None):
        self.config = config
        # Init logger for output to file
        self.logger = logger or get_logger(
            name='Extract',
            log_file='../logs/etl.log',
            level=logging.INFO
        )
    

    def extract(self, file_name: str):
        # Determine file type and call relevant method
        file_type = file_name.split('.')[-1].lower()
        self.logger.info(f"Extract: loading data as {file_type}...")

        if file_type == 'csv':
            return self.extract_csv(file_name)
        elif file_type == 'json':
            return self.extract_json(file_name)
        else:
            self.logger.error(f"Extract: Unsupported file type: {file_type}")
            raise ValueError(f"Extract: Unsupported file type: {file_type}")

    def extract_csv(self, file_path):
        self.logger.info(f"Extract: loading CSV data from {file_path}...")
        data = pd.read_csv(file_path)
        self.logger.info(f"Extract: loaded CSV data: {data.shape}.")
        self.logger.info(f"Extract: Columns: {list(data.columns)}")
        #self.logger.info(f"Extract: First 5 rows:\n{data.head()}")


        _log_preview(self.logger, data)
        return data

    def extract_json(self, file_path):
        self.logger.info(f"Extract: loading JSON data from {file_path}...")
        data = pd.read_json(file_path)
        self.logger.info(f"Extract: loaded JSON data: {data.shape}.")
        self.logger.info(f"Extract: Columns: {list(data.columns)}")
        #self.logger.info(f"Extract: First 5 rows:\n{data.head()}")

        _log_preview(self.logger, data)
        return data
