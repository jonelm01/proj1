## ETL Pipeline Project

A modular Extract–Transform–Load (ETL) pipeline built in Python.
The project processes dirty, raw cafe sales data, validates and cleans it, and loads it into an output file and a PostgreSQL database.
It includes full test coverage via pytest, structured logging, config management, and simple CLI execution.

## Installation
1. Clone the repo
```
git clone https://github.com/jonelm01/proj1.git 
cd proj1
```

3. Install dependencies
```pip install -r requirements.txt```

4. Configure environment variables

If your ETL loads into PostgreSQL, set:
```
export DB_HOST=localhost
export DB_USER=postgres
export DB_PASS=yourpassword
export DB_NAME=etl_db
```

# Usage
Run the ETL pipeline from the project root with:

```python -m src.main```


Or call the function directly:
```
from src.main import run_etl
run_etl()
```

# Extract
- Loads raw CSV data from data/in/
- Validates source structure
- Reads using pandas

# Transform
- Cleans data (types, formats, column normalization)
- Fixes invalid and empty dates
- Computes missing values where possible
- Logs intermediate transformations
- Fully tested and mockable

# Load
- Writes cleaned CSVs to data/out/
- Attempts to load into PostgreSQL using psycopg2
- Separates valid and rejected rows
- Outputs rejects table and cleaned, valid tables

# Logging
- All ETL steps write structured logs to /logs/etl.log
- Shared logger via get_logger() in src.util

# Testing
- Full test suite via pytest
- Mocks included for database and file systems
- Coverage via terminal and HTML output in htmlcov/

## Project Structure
```
.
├── config/
│   ├── config.json
│   └── sources.yaml
├── data/
│   ├── in/                # raw input CSV files
│   └── out/               # cleaned output (ignored by Git)
├── logs/
│   └── etl.log
├── src/
│   ├── extract.py         # DataExtractor
│   ├── transform.py       # Transformer
│   ├── load.py            # Loader + RejectsLoader
│   ├── db_conn.py         # PostgreSQL connection logic
│   ├── util.py            # shared utilities + logger
│   ├── main.py            # run_etl() entrypoint
│   └── validate.py        # schema + validation logic
└── tests/
    ├── test_extract.py
    ├── test_transform.py
    ├── test_load.py
    ├── test_validate.py
    ├── test_main.py
    └── conftest.py
```

# Testing

Run all tests:

```
pytest -q 
```

Generate coverage:
```
pytest --cov=src --cov-report=term-missing --cov-report=html
```

Coverage report will appear in:

htmlcov/index.html
