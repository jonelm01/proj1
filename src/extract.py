import pandas as pd
import logging
from src.util import get_logger, _log_preview
import os


class DataExtractor:
    def __init__(self, logger=None):
        # Init logger for output to file
        self.logger = logger or get_logger(
            name='Extract',
            log_file='../logs/etl.log',
            level=logging.INFO
        )

    def extract(self, file_name: str):
        # Determine file type and call relevant method
        file_type = file_name.split('.')[-1].lower()
        self.logger.info(f"Loading data as {file_type}...")

        if file_type == 'csv':
            return self.extract_csv(file_name)
        elif file_type == 'json':
            return self.extract_json(file_name)
        else:
            self.logger.error(f"Unsupported file type: {file_type}")
            raise ValueError(f"Unsupported file type: {file_type}")

    def extract_csv(self, file_path):
        self.logger.info(f"Loading CSV data from {file_path}...")
        data = pd.read_csv(file_path)

        _log_preview(self.logger, data)
        return data

    def extract_json(self, file_path):
        self.logger.info(f"Loading JSON data from {file_path}...")
        data = pd.read_json(file_path)

        _log_preview(self.logger, data)
        return data
