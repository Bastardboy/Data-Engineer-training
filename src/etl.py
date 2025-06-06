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
        with open(SCRIPT_PATH, 'r', encoding='utf-8') as file:
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

def formate_timestamp(date_data):
    """
    Function to transform the $numberLong to a datetime (mongodb format to datetime)
    2 cases:
     2.1 -> date is in numberlong format {"$date": {"$numberLong":-199843200000}}
     2.2 -> date is in isoformat {"$date": "2020-01-01T00:00:00.000Z"}
     Both are inside of a dict with the key "$date" so we need to check that
    """
    if not isinstance(date_data, dict) or '$date' not in date_data:
        return None

    value = date_data['$date']

    if isinstance(value, dict) and '$numberLong' in value:
        try:
            return datetime.fromtimestamp(int(value['$numberLong']) / 1000)
        except Exception:
            return None

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
    
    cursor.execute("SELECT name_symbol, ID_SYMBOL FROM DIM_SYMBOL")
    save_rows = cursor.fetchall()
    symbol_map = {row[0]: row[1] for row in save_rows}
    print(f"Symbol map: {symbol_map}")
    return symbol_map

def transformation_dim_customers(conn, customers_data):
    """
    Now we need to extract the customers data
    ALSO need to:
    1. normalize the 'tier and details' and 'benefits' fields
    2. formated date of birth (can be numberlong or isoformat again) with the prev function
    """
    cursor = conn.cursor()
    register_customers = []
    username_and_name = set()

    for customer in customers_data:
        username = customer.get('username')
        name = customer.get('name')

        concatenation_username_name = f"{username}_{name}"

        if not username or not name:
            print(f"Error: customer {concatenation_username_name} is missing username or name. Skipping.")
            continue
        if concatenation_username_name in username_and_name:
            print(f"Error: customer {concatenation_username_name} already exists. Skipping.")
            continue
        username_and_name.add(concatenation_username_name)

        birth_date = customer.get('birthdate')
        if birth_date:
            birth_date_formated = formate_timestamp(birth_date) 
            if birth_date_formated:
                birth_date = birth_date_formated.date().isoformat()
            else:
                print(f"Error: birthdate {birth_date} could not be transformed.")
                birth_date = None
        
        tier = None
        benefits = set()
        tier_and_details = customer.get('tier_and_details', {})

        for data in tier_and_details.values():
            if data.get('active', False):
                tier = data.get('tier')
                benefits.update(data.get('benefits', []))

        if benefits:
            benefits = list(benefits) 
            benifits_str = json.dumps(benefits)
        else:
            benifits_str = None

    
        register_customers.append((name, username, concatenation_username_name, birth_date, tier, benifits_str))

    # solved -> An unexpect error happens: Error binding parameter 4 - probably unsupported type.
    # solved ->Database error while inserting customers: UNIQUE constraint failed: DIM_CUSTOMERS.username
    try:
        cursor.executemany("""
            INSERT INTO DIM_CUSTOMERS (name_customer, username, customer_natural_key, birthdate, tier, benefits) VALUES (?, ?, ?, ?, ?, ?)""", register_customers)
        conn.commit()
        print(f"Inserted {len(register_customers)} customers into DIM_CUSTOMERS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting customers: {e}")

# Database error while inserting customers: UNIQUE constraint failed: DIM_CUSTOMERS.username
    """
    Okay, now i understand we need to map the data from the different tables
    with this we can make functions to get the data from the tables (using the fk i define prev)
    sooo the result be like {username: id_customer}
    """
    cursor.execute("SELECT customer_natural_key, ID_CUSTOMER FROM DIM_CUSTOMERS")

    save_rows = cursor.fetchall()
    customer_map = {row[0]: row[1] for row in save_rows}
    return customer_map

def transformation_dim_accounts(conn, accounts_data):
    """
    Now we need to extract the accounts data
    in this case accounts_id are unique
    limit of account never is 0 or null, so we can use get('limit)
    and prodcuts are in a list [], so we can use str to convert it to a string
    well because we have 1 repet account id, time to use a set
    """
    cursor = conn.cursor()
    register_accounts = []
    accounts_id_visited = set()

    for account in accounts_data:
        id_account = account.get('account_id')
        limit = account.get('limit', 0)
        products_list = account.get('products')

        if id_account in accounts_id_visited:
            print(f"Advertencia: account_id '{id_account}' duplicado en los datos de origen. Ignorando esta instancia.")
            continue
        accounts_id_visited.add(id_account)    

        # Difference between previous function, products can be same but on different accounts
        # but now i find a account who have same products but different orden, so a sorted should help for all i guess
        # sorted dindt help, so i go back to str to add in the same orden on json
        if products_list and isinstance(products_list, list):
            products_sorted = json.dumps(products_list)

        register_accounts.append((id_account, limit, products_sorted))    


    # now we fix some words on script (esp to eng)
    # exist 1746 accounts_id on jsonfile where 627788 is repeted
    # so i set for get 1745, because this two have the same products different order

    try:
        cursor.executemany("""
            INSERT INTO DIM_ACCOUNTS (id_account, limit_budget, products) VALUES (?, ?, ?)""", register_accounts)
        conn.commit()
        print(f"Inserted {len(register_accounts)} accounts into DIM_ACCOUNTS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting accounts: {e}")

    cursor.execute("SELECT id_account, ID_ACCOUNT_UNIQUE FROM DIM_ACCOUNTS")
    save_rows = cursor.fetchall()
    account_map = {row[0]: row[1] for row in save_rows}
    return account_map


def transformation_dim_type_transactions(conn, transactions_data):
    """ 
    Now time to extract the tot (type of transaction) (buy or sell) from the transactions
    """
    cursor = conn.cursor()
    unique_tot = set()

    for type_transaction in transactions_data:
        for transactions in type_transaction['transactions']:
            transaction_code = transactions.get('transaction_code')
            if transaction_code:
                unique_tot.add(transaction_code)

    register_type_transactions = []
    for tot in sorted(unique_tot):
        register_type_transactions.append((tot,))

    try:
        cursor.executemany("""
            INSERT INTO DIM_TYPE_TRANSACTIONS (name_type_transacion) VALUES (?)""", register_type_transactions)
        conn.commit()
        print(f"Inserted {len(register_type_transactions)} unique transaction types into DIM_TYPE_TRANSACTIONS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting transaction types: {e}")

    cursor.execute("SELECT name_type_transacion, ID_TYPE_TRANSACTION FROM DIM_TYPE_TRANSACTIONS")
    save_rows = cursor.fetchall()
    type_transaction_map = {row[0]: row[1] for row in save_rows}
    print(f"Type transaction map: {type_transaction_map}")
    return type_transaction_map

def transformation_fact_transactions(conn, transactions_data):
    """
    now is time to create the center table of the star,
    but for that we need use the information of the previous tables

    my ideas
    - use te data from the other json is not viable, to much processing so rip code
    - i should put the formated data on variables sooo i can reuse, but for now lets going to test the first json
      - ** the data is raw when i use variable, so i need a estrcuture to store the data, which should be a dictionary
    - re use the structure from prev functions is ideal, but this function is going to have much other steps
    - okay time to shine
    """
    cursor = conn.cursor()
    fact_records_to_insert = []

    
    for account_trans in transactions_data:
        account_id_og = account_trans['account_id']
        print(f"Processing account_id: {account_id_og}")
    
    print(f'We entered to transformation_fact_transactions with {len(transactions_data)} transactions')

    

def run_etl():
    """Main function, where all the ETL process is going to be executed."""
    conn = None
    try:
        conn = connect_to_db(DB_NAME)
        create_tables(conn) 

        print("\n--- 1st Stage: Extracting data from our jsons ---")
        accounts_data = load_json_file(ACCOUNTS_DATA) # 1746 ; 627788 is repeted
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

        transformation_dim_customers(conn, customers_data)
        transformation_dim_accounts(conn, accounts_data)

        transformation_dim_type_transactions(conn, transactions_data)
        #transformation_fact_transactions(conn, transactions_data)

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
