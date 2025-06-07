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
QUERY_SQL_PATH = os.path.join(DB_DIR, "query.sql")

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

def formate_timestamp(date_data):
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


def transformation_dim_dates(conn, transactions_data):

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
            INSERT OR IGNORE INTO DIM_DATES (id_date, full_date, year, month, day) VALUES (?, ?, ?, ?, ?)""", registerd_dates)
        conn.commit()
        print(f"Inserted {len(registerd_dates)} unique dates into DIM_DATES.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting dates: {e}")
        conn.rollback()

def transformation_dim_symbol(conn, transactions_data):
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
            INSERT OR IGNORE INTO DIM_SYMBOL (name_symbol) VALUES (?)""", registred_symbols)
        conn.commit()
        print(f"Inserted {len(registred_symbols)} unique symbols into DIM_SYMBOL.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting symbols: {e}")
        conn.rollback()
    
    cursor.execute("SELECT name_symbol, ID_SYMBOL FROM DIM_SYMBOL")
    save_rows = cursor.fetchall()
    symbol_map = {row[0]: row[1] for row in save_rows}
    print(f"Symbol map: {symbol_map}")
    return symbol_map

def transformation_dim_customers(conn, customers_data):
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

    try:
        cursor.executemany("""
            INSERT  OR IGNORE INTO DIM_CUSTOMERS (name_customer, username, customer_natural_key, birthdate, tier, benefits) VALUES (?, ?, ?, ?, ?, ?)""", register_customers)
        conn.commit()
        print(f"Inserted {len(register_customers)} customers into DIM_CUSTOMERS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting customers: {e}")
        conn.rollback()

    cursor.execute("SELECT customer_natural_key, ID_CUSTOMER FROM DIM_CUSTOMERS")
    save_rows = cursor.fetchall()
    customer_map = {row[0]: row[1] for row in save_rows}
    print(f"Customer map example (5 entries): {list(customer_map.items())[:5]}")
    return customer_map


def relation_customers_and_accounts(customers_data):
    account_customer_map = {}
    for customer in customers_data:
        username = customer.get('username')
        name = customer.get('name')
        customer_natural_key = f"{username}_{name}"

        for account_id in customer.get('accounts', []):
            account_customer_map[account_id] = customer_natural_key
    print(f"Created account-customer map with {len(account_customer_map)} entries.")
    print("Example of account-customer map:", list(account_customer_map.items())[:5])
    return account_customer_map



def transformation_dim_accounts(conn, accounts_data, map_customers_and_accounts, dim_customers_map):
    cursor = conn.cursor()
    register_accounts = []

    for account in accounts_data:
        id_account = account.get('account_id')
        limit = account.get('limit', 0)
        products_list = account.get('products')

        products_sorted = None 
        if products_list and isinstance(products_list, list):
            products_sorted = json.dumps(products_list)
        
        customer_natural_key = map_customers_and_accounts.get(id_account)

        customer_id = None 
        if customer_natural_key:
            customer_id = dim_customers_map.get(customer_natural_key)

        if customer_id is None:
            print(f"Error: customer for account {id_account} (natural key: {customer_natural_key}) not found. Skipping account.")
            continue

        register_accounts.append((id_account, customer_id, limit, products_sorted))

    try:
        cursor.executemany("""
            INSERT  OR IGNORE INTO DIM_ACCOUNTS (id_account, customer_id, limit_budget, products) VALUES (?,?, ?, ?)""", register_accounts)
        conn.commit()
        print(f"Inserted {len(register_accounts)} accounts into DIM_ACCOUNTS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting accounts: {e}")
        conn.rollback()

    cursor.execute("SELECT id_account, ID_ACCOUNT_UNIQUE FROM DIM_ACCOUNTS")
    save_rows = cursor.fetchall()
    account_map = {row[0]: row[1] for row in save_rows}
    print(f"Account map example (5 entries): {list(account_map.items())[:5]}")
    return account_map


def transformation_dim_type_transactions(conn, transactions_data):
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
            INSERT  OR IGNORE INTO DIM_TYPE_TRANSACTIONS (name_type_transacion) VALUES (?)""", register_type_transactions)
        conn.commit()
        print(f"Inserted {len(register_type_transactions)} unique transaction types into DIM_TYPE_TRANSACTIONS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting transaction types: {e}")
        conn.rollback()

    cursor.execute("SELECT name_type_transacion, ID_TYPE_TRANSACTION FROM DIM_TYPE_TRANSACTIONS")
    save_rows = cursor.fetchall()
    type_transaction_map = {row[0]: row[1] for row in save_rows}
    print(f"Type transaction map: {type_transaction_map}")
    return type_transaction_map

def transformation_fact_transactions(conn, transactions_data, map_customers_and_accounts ,dim_accounts_map, dim_customers_map, dim_symbol_map, dim_tot_map):
    cursor = conn.cursor()
    fact_records_to_insert = []

    for account_trans in transactions_data:
        account_id_og = account_trans['account_id']
        account_id_unique = dim_accounts_map.get(account_id_og)

        customers_accounts = map_customers_and_accounts.get(account_id_og)

        customers_id_og = dim_customers_map.get(customers_accounts)

        for trans in account_trans['transactions']:
            date_formated = formate_timestamp(trans['date'])

            if not date_formated:
                print(f"Error: cant determinate date for {trans['date']}. Skipping transaction.")
                continue

            date_id = int(date_formated.strftime('%Y%m%d'))

            amount = float(trans['amount'])
            price_og = trans.get('price')
            if price_og is not None and str(price_og).strip() != "":
                price = float(price_og) 
            else:
                price = None

            total_og = trans.get('total')
            if total_og is not None and str(total_og).strip() != "":
                total = float(total_og)
            else:
                total = None

            trans_code = trans['transaction_code']
            tot_id = dim_tot_map.get(trans_code)

            symbol = trans.get('symbol')
            if symbol:
                symbol_id = dim_symbol_map.get(symbol)
            else:
                symbol_id = None

            fact_records_to_insert.append((
                date_id,
                customers_id_og,
                account_id_unique,
                tot_id,
                symbol_id,
                amount,
                price,
                total,
                trans_code,
                date_formated.isoformat()
            ))
    
    try:
        cursor.executemany("""
            INSERT  OR IGNORE  INTO FACT_TRANSACTIONS (date_id, customer_id, account_id, type_transaction_id, symbol_id, amount, price, total, transaction_code, transaction_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", fact_records_to_insert)
        conn.commit()
        print(f"Inserted {len(fact_records_to_insert)} records into FACT_TRANSACTIONS.")
    except sqlite3.DatabaseError as e:
        print(f"Database error while inserting transactions: {e}")
        conn.rollback()
            

def query_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT limit_budget FROM DIM_ACCOUNTS;")
    budgets = [row[0] for row in cursor.fetchall() if row[0] is not None]

    if budgets:
        avg = sum(budgets) / len(budgets)
        min_val = min(budgets)
        max_val = max(budgets)
        # Desviación estándar muestral
        if len(budgets) > 1:
            mean = avg
            stddev = (sum((x - mean) ** 2 for x in budgets) / (len(budgets) - 1)) ** 0.5 # <--- ¡Aquí está!
        else:
            stddev = 0
        print("Results:")
        print(f"Averange of limit budget: {avg:.2f}")
        print(f"Min límite: {min_val:.2f}")
        print(f"Max límite: {max_val:.2f}")
        print(f"STDEV: {stddev:.2f}") # <--- Y aquí se imprime
    else:
        print("No hay datos en DIM_ACCOUNTS.")

    try:
        # Abre y lee el contenido del archivo query.sql
        with open(QUERY_SQL_PATH, 'r', encoding='utf-8') as file:
            sql_script_content = file.read()

        individual_queries = [q.strip() for q in sql_script_content.split(';') if q.strip()]

        for i, query_text in enumerate(individual_queries):
            print(f"\n--- Results for querys {i+1} ---")
            
            try:
                cursor.execute(query_text)
                results = cursor.fetchall()

                if i == 0:
                    print(f'The query " ¿Cuál es el Promedio, mínimo, máximo y desviación estándar del limite de las cuentas de los clientes?" was executed by python code, see previous print.')
                    continue
                elif results:
                    column_names = [description[0] for description in cursor.description]
                    print(f"Columns: {', '.join(column_names)}")
                    for row in results:
                        formatted_row = " | ".join(str(item) for item in row)
                        print(formatted_row)
                else:
                    print("No results found for this query.")
            except sqlite3.OperationalError as e:
                print(f"Error executing the SQL query: {e}")
            except sqlite3.DatabaseError as e:
                print(f"Database error while executing the query: {e}")
            except Exception as e:
                print(f"An unexpected error occurred during query execution: {e}")

    except FileNotFoundError:
        print(f"Error: SQL query file not found at {QUERY_SQL_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred while reading or processing query.sql: {e}")

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
