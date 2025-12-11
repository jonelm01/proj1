## ETL Pipeline Project

A modular Extract–Transform–Load (ETL) pipeline built in Python.
The project processes dirty, raw cafe sales data, validates, cleans, and normalizes it, and loads it into an output file and a PostgreSQL database. Analytics, summaries, etc are viewable in a Streamlit dashboard.
It includes full test coverage via pytest, structured logging, config management, and simple CLI execution.

## Installation
Clone the repo
```
git clone https://github.com/jonelm01/proj1.git 
cd proj1
```

Install dependencies
```pip install -r requirements.txt```

Configure .env

```
DB_HOST=host
DB_NAME=db_name
DB_USER=user
DB_PASS=pw
DB_PORT=####
```

# Usage
Run the ETL pipeline from the project root with:

```python -m streamlit run src/main.py --logger.level=info```

 Or, with no logging:

```python -m streamlit run src/main.py```

# Schema Driven
 - Schema, cleaning rules, required fields, and normalization configuration are fully managed via `config/sources.yml`.
 - config/sources.yml controls:
    - Source schema (columns, data types, primary keys)
    - Cleaning rules (required fields, missing values, transformations)
    - Normalization rules (dimensions, surrogate keys, fact tables)
    - Load targets (Postgres table mappings)

# Extract
- Loads raw CSV data from data/in/
- Validates source structure
- Reads as pandas df

# Transform
- Cleans data (types, formats, column normalization)
- Fixes invalid and empty dates
- Computes missing values where possible
- Standardizes bad values
- Safely converts types
- Normalizes into fact and domain tables
- Domain checks and required field validation
- Splits clean and rejected rows
- Logs intermediate transformations

# Load
- Attempts to load/upsert into PostgreSQL using psycopg2
- Outputs rejects table and cleaned, valid tables

# Logging and Monitoring
- All ETL steps write structured logs to /logs/etl.log
- Shared logger via get_logger() in src.util
- Optional Streamlit dashboard for inspecting processed data and ETL metrics

# Testing
- Full test suite via pytest
- Mocks included for database and file systems
- Coverage via terminal and HTML output in htmlcov/

## Project Structure
```
.
├── README.md
├── config
│   └── sources.yml
├── data
│   └── in
│       └── dirty_cafe_sales.csv
├── htmlcov
├── logs
│   └── etl.log
├── pytest.ini
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── analytics.py
│   ├── db_conn.py
│   ├── extract.py
│   ├── load.py
│   ├── main.py
│   ├── pages
│   │   └── logs.py
│   ├── transform.py
│   └── util.py
└── tests
    ├── __pycache__
    ├── test_analytics.py
    ├── test_conn.py
    ├── test_extract.py
    ├── test_load.py
    ├── test_main.py
    ├── test_transform.py
    └── test_validate.py

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
