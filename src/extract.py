import pandas as pd
import logging
import json
from pathlib import Path
from src.util import get_logger
import os

class DataExtractor:
    def __init__(self,  logger=None):
        if logger:
            self.logger = logging.getLogger("Extract")
            self.logger.setLevel(logger.level)
            for h in logger.handlers:
                if h not in self.logger.handlers:
                    self.logger.addHandler(h)
        else:
            self.logger = get_logger(
                name='Extract',
                log_file='../logs/etl.log',
                level=logging.INFO
            )
        self.logger.info("------------------------ Extractor initialized -----------------------")
    

    def extract(self, file_name: str):
        # Determine file type and call relevant method for non-Streamli version
        file_type = file_name.split('.')[-1].lower()
        self.logger.info(f"extract: Extracting data as {file_type}...")

        if file_type == 'csv':
            return self.extract_csv(file_name)
        elif file_type == 'json':
            return self.extract_json(file_name)
        else:
            self.logger.error(f"extract: Unsupported file type: {file_type}")
            raise ValueError(f"extract: Unsupported file type: {file_type}")


    def extract_csv(self, file_path):
        self.logger.info(f"extract: Extracting CSV data from {file_path}...")
        data = pd.read_csv(file_path)
        
        self.logger.info(f"extract: Successfully extracted CSV data: {data.shape}.")
        self.logger.info(f"extract: Columns: {list(data.columns)}")
        self.logger.info("------------------------ Extraction complete -----------------------")

        return data


    def extract_json(self, file_path):
        self.logger.info(f"extract: Extracting JSON data from {file_path}...")
        data = pd.read_json(file_path)
        
        self.logger.info(f"extract: Successfully extracted JSON data: {data.shape}.")
        self.logger.info(f"extract: Columns: {list(data.columns)}")
        self.logger.info("------------------------ Extraction complete -----------------------")

        return data