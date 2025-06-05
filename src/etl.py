import sqlite3
import pandas as pd
import os
import json

# Define the roots to the directories we use
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "sample_analytics_dataset")

# Path to our json data files, so we can load them
ACCOUNTS_DATA = os.path.join(DATA_DIR, "sample_analytics.accounts.json")
CUSTOMERS_DATA = os.path.join(DATA_DIR, "sample_analytics.customers.json")
TRANSACTIONS_DATA = os.path.join(DATA_DIR, "sample_analytics.transactions.json")

# Path to script sql to create the tables in the database
SCRIPT_PATH = os.path.join(ROOT_DIR, "script.sql")

SQL_PATH = os.path.join(ROOT_DIR, SCRIPT_PATH)
DB_NAME = "dw_financial.db"

'''
Now we are going to modularize the code, because we need to
reuse variables, like conn the connection
'''

def connect_to_db(db_name):
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = ON;") 
    return conn

def create_tables(conn):
    try:
        cursor = conn.cursor()
        with open(SQL_PATH, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        cursor.executescript(sql_script)
        conn.commit()
        print("The tables have been created successfully.")
    except FileNotFoundError:
        print(f"Error: SQL script file not found at {SQL_PATH}")
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def load_json_file(file_path):
    """Reads the JSON files and check if have all the information"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: file was not found on {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: JSON cant be decode {file_path}")
        return None


def run_etl():
    """Main function, where all the ETL process is going to be executed."""
    conn = None
    try:
        conn = connect_to_db(DB_NAME)
        create_tables(conn) 

        print("\n--- 1st Stage: Extracting data from our jsons ---")
        accounts_data = load_json_file(ACCOUNTS_DATA) # 1746
        customers_data = load_json_file(CUSTOMERS_DATA) # 500
        transactions_data = load_json_file(TRANSACTIONS_DATA) # 1746

        # data = pd.DataFrame(accounts_data)
        # data2 = pd.DataFrame(customers_data)
        # data3 = pd.DataFrame(transactions_data)
        # print(f"Accounts data loaded: {len(data)} records")
        # print(f"Customers data loaded: {len(data2)} records")
        # print(f"Transactions data loaded: {len(data3)} records")

        if not accounts_data or not customers_data or not transactions_data:
            print("Error: One or more files couldnt be loaded. ETL Process aborted.")
            return
    except Exception as e:
        print(f"An unexpect error happens: {e}")
    finally:
        if conn:
            conn.close()
            print("The conexion to database was closed.")

if __name__ == "__main__":
    run_etl()
