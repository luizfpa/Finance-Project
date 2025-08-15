import sqlite3
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker with seed for reproducibility
fake = Faker()
Faker.seed(42)

# Connect to SQLite database
db_path = "/home/luizfp22/projects/data-generation-project/Finance-Project/database/personal_finance.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create transactions table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        amount REAL NOT NULL,
        category TEXT NOT NULL,
        description TEXT
    )
""")

# Define categories
categories = ["Food", "Transport", "Entertainment", "Bills", "Shopping"]

# Generate 1000 records
for _ in range(1000):
    # Generate random date within the last year
    date = fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    # Ensure amount is positive and realistic
    amount = round(random.uniform(5.0, 500.0), 2)
    category = random.choice(categories)
    description = fake.sentence(nb_words=6)
    
    cursor.execute("""
        INSERT INTO transactions (date, amount, category, description)
        VALUES (?, ?, ?, ?)
    """, (date, amount, category, description))

# Commit and close
conn.commit()
conn.close()
print("Generated 1000 transaction records in personal_finance.db")