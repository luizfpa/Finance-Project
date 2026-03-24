# Personal Finance ETL Project

This project implements a Python-based ETL (Extract, Transform, Load) pipeline for personal finance data. It is designed to be orchestrated using **Apache Airflow** and stores transaction data in **SQLite** databases.

## Project Structure

- `finance_etl_dag.py`: The Airflow DAG definition that schedules and runs the ETL pipeline.
- `my_etl_utils.py`: A utility module containing the core functions for data extraction, transformation (aggregation/cleaning), and loading.
- `generate_finance_data.py`: A script to populate the source database with mock transaction data using the `Faker` library.
- `test_etl_utils.py`: A test script to verify that the ETL functions work correctly in your environment.
- `database/`: A directory intended to hold the SQLite database files.

## Prerequisites

Ensure you have Python 3 installed. The following libraries are required:

```bash
pip install pandas faker apache-airflow pendulum
```

## Getting Started

### 1. Generate Mock Data
Before running the ETL, you need to populate the source database:

```bash
python3 generate_finance_data.py
```
This will create (if it doesn't exist) `database/personal_finance.db` and insert 1000 mock transactions.

### 2. Run Tests
To verify the ETL logic locally:

```bash
python3 test_etl_utils.py
```
This script will perform a sample extraction, aggregate the data by category, and load the results into the `processed_transactions` table.

### 3. Airflow Setup
To run the full pipeline in Airflow:
1. Copy the project files to your Airflow `dags` folder or add the path to `PYTHONPATH`.
2. Configure an Airflow connection named `finance_db` pointing to your SQLite database.
3. (Optional) Set an Airflow variable `finance_db_path` to customize the database location (defaults to `database/personal_finance.db`).

## Key Features

- **Portability:** Uses relative paths to ensure it works across different environments without modification.
- **Incremental Loading:** The Airflow DAG is configured to process data month-by-month based on the execution date.
- **Schema Validation:** The transformation step automatically checks for relevant columns (e.g., `category`, `amount`, `product_name`) to decide how to aggregate data.
- **Data Privacy:** Hardcoded absolute paths and sensitive data have been removed.

## License
This project is licensed under the terms of the LICENSE file included in the repository.
