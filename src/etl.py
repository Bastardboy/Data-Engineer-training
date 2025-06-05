import sqlite3
import os
import json
from datetime import datetime
# Define the roots to the directories we use
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "sample_analytics_dataset")
DB_DIR = os.path.join(ROOT_DIR, "db")

# Path to our json data files, so we can load them
ACCOUNTS_DATA = os.path.join(DATA_DIR, "sample_analytics.accounts.json")
CUSTOMERS_DATA = os.path.join(DATA_DIR, "sample_analytics.customers.json")
TRANSACTIONS_DATA = os.path.join(DATA_DIR, "sample_analytics.transactions.json")

# Path to script sql to create the tables in the database
SCRIPT_PATH = os.path.join(DB_DIR, "script.sql")

SQL_PATH = os.path.join(ROOT_DIR, SCRIPT_PATH)
DB_NAME = "dw_financial.db"

"""
Now we are going to modularize the code, because we need to
reuse variables, like conn the connection
"""

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

# Function to transform the $numberLong to a datetime (mongodb format to datetime)
def formate_timestamp(date_data):
    """
    2 cases:
     2.1 -> date is in numberlong format {"$date": {"$numberLong":-199843200000}}
     2.2 -> date is in isoformat {"$date": "2020-01-01T00:00:00.000Z"}
     Both are inside of a dict with the key "$date" so we need to check that
    """
    if not isinstance(date_data, dict) or '$date' not in date_data:
        return None

    value = date_data['$date']

    # Caso 1: timestamp en milisegundos
    if isinstance(value, dict) and '$numberLong' in value:
        try:
            return datetime.fromtimestamp(int(value['$numberLong']) / 1000)
        except Exception:
            return None

    # Caso 2: string ISO
    if isinstance(value, str):
        try:
            return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
        except Exception:
            return None

    return None

""" Here i going to define the functons to transform data for each db table"""

def transformation_dim_dates(conn, transactions_data):
    """
    Now is time to extract the date from sample_analytics.transactions.json to insert on dim_dates
    Using a set to avoid duplicates dates
    """
    cursor = conn.cursor()
    uniques_dates = set()

    for accounts in transactions_data:
        for transaction in accounts['transactions']:
            date_formated = formate_timestamp(transaction['date'])

            if date_formated:
                uniques_dates.add(date_formated.date())
            else:
                print(f"Error: date {transaction['date']} could not be transformed.")

    registerd_dates = []
    for date in sorted(uniques_dates):
        id_date = int(date.strftime('%Y%m%d'))
        full_date = date.isoformat()
        year = date.year
        month = date.month
        day = date.day

        registerd_dates.append((id_date, full_date, year, month, day))

    try:
        cursor.executemany("""
            INSERT INTO DIM_DATES (id_date, full_date, year, month, day) VALUES (?, ?, ?, ?, ?)""", registerd_dates)
        conn.commit()
        print(f"Inserted {len(registerd_dates)} unique dates into DIM_DATES.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting dates: {e}")

def trasnformation_dim_symbol(conn, transactions_data):
    """
    Now we need to extract the symbols (amz, nvd, etc)
    """

    cursor = conn.cursor()
    uniques_symbols = set()

    for accounts in transactions_data:
        for transaction in accounts['transactions']:
            symbol = transaction.get('symbol')
            if symbol:
                uniques_symbols.add(symbol)
    
    registred_symbols = []
    for symbol in sorted(uniques_symbols):
        registred_symbols.append((symbol,))

    try:
        cursor.executemany("""
            INSERT INTO DIM_SYMBOL (name_symbol) VALUES (?)""", registred_symbols)
        conn.commit()
        print(f"Inserted {len(registred_symbols)} unique symbols into DIM_SYMBOL.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting symbols: {e}")

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
        
        print("\n--- 2nd Stage: Trasformations and loaded of data ---") 
        # here we are going to use the functions to transform and load the data

        transformation_dim_dates(conn, transactions_data)
        trasnformation_dim_symbol(conn, transactions_data)
    
    except sqlite3.DatabaseError as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
            print("Transaction rolled back due to database error.")
    except Exception as e:
        print(f"An unexpect error happens: {e}")
    finally:
        if conn:
            conn.close()
            print("The conexion to database was closed.")

if __name__ == "__main__":
    run_etl()
