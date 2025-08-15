import sqlite3
import pandas as pd
import logging
from my_etl_utils import extract_data, transform_data, load_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database paths
finance_db_path = "/home/luizfp22/projects/data-generation-project/Finance-Project/database/personal_finance.db"

def test_etl_finance():
    """Tests the ETL for the finance database."""
    try:
        query = "SELECT * FROM transactions WHERE amount > 0 LIMIT 5"
        data = extract_data(finance_db_path, query)
        logging.info(f"Extracted data:\n{data}")

        transformed_data = transform_data(data, transformation_type="aggregate", db_path=finance_db_path, table_name="transactions")
        logging.info(f"Transformed data:\n{transformed_data}")

        load_data(finance_db_path, transformed_data, "processed_transactions")
        logging.info("Finance ETL test completed.")
    except Exception as e:
        logging.error(f"Error in finance test: {str(e)}")

if __name__ == "__main__":
    # Create destination tables if they don't exist
    for db_path, table in [
        (finance_db_path, "processed_transactions"),
    ]:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                category TEXT,
                amount REAL
            )
        """)
        conn.commit()
        conn.close()

    # Run tests
    test_etl_finance()