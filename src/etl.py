import sqlite3
import os
from utils.database import connect_to_db, create_tables
from utils.loader import load_json_file
from utils.transform_dates import transformation_dim_dates
from utils.transform_symbols import transformation_dim_symbol
from utils.transform_customers import transformation_dim_customers, relation_customers_and_accounts
from utils.transform_accounts import transformation_dim_accounts
from utils.transform_tot import transformation_dim_type_transactions
from utils.transform_transactions import transformation_fact_transactions
from utils.execute_query import query_data

# Define the roots to the directories we use
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Where data is stored
DATA_DIR = os.path.join(ROOT_DIR, "sample_analytics_dataset")
ACCOUNTS_DATA = os.path.join(DATA_DIR, "sample_analytics.accounts.json")
CUSTOMERS_DATA = os.path.join(DATA_DIR, "sample_analytics.customers.json")
TRANSACTIONS_DATA = os.path.join(DATA_DIR, "sample_analytics.transactions.json")

# Where we want to store the database
DB_DIR = os.path.join(ROOT_DIR, "db")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "dw_financial.db")
DB_NAME = "dw_financial.db"

def run_etl():
    conn = None
    try:
        conn = connect_to_db(DB_NAME)
        create_tables(conn) 

        print("\n--- 1st Stage: Extracting data from our jsons ---")
        accounts_data = load_json_file(ACCOUNTS_DATA) # 1746 ; 627788 is repeted -> 1745
        customers_data = load_json_file(CUSTOMERS_DATA) # 500
        transactions_data = load_json_file(TRANSACTIONS_DATA) # 1746 -> if we separate for each transaction we have 88119

        if not accounts_data or not customers_data or not transactions_data:
            print("Error: One or more files couldnt be loaded. ETL Process aborted.")
            return
        
        print("\n--- 2nd Stage: Trasformations and loaded of data ---") 

        transformation_dim_dates(conn, transactions_data)
        
        dim_symbol_map = transformation_dim_symbol(conn, transactions_data)

        dim_customers_map = transformation_dim_customers(conn, customers_data)
        map_customers_and_accounts = relation_customers_and_accounts(customers_data)
        dim_accounts_map = transformation_dim_accounts(conn, accounts_data, map_customers_and_accounts, dim_customers_map)

        dim_tot_map = transformation_dim_type_transactions(conn, transactions_data)
        
        print("\n--- 3rd Stage: time to create table transactions ---")
        transformation_fact_transactions(conn, transactions_data, map_customers_and_accounts, dim_accounts_map, dim_customers_map, dim_symbol_map, dim_tot_map)

        print("\n--- 4th Stage: Querying data ---")
        query_data(conn)

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
