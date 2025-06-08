import sqlite3, os, json, logging, sys
from datetime import datetime

# Configure logging
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(module)s:%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    for handler in [logging.StreamHandler(sys.stdout), logging.FileHandler('etl.log', mode='w')]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logging()

# Define directory paths
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, "sample_analytics_dataset")
DB_DIR = os.path.join(ROOT_DIR, "db")
# JSON data file paths
ACCOUNTS_DATA = os.path.join(DATA_DIR, "sample_analytics.accounts.json")
CUSTOMERS_DATA = os.path.join(DATA_DIR, "sample_analytics.customers.json")
TRANSACTIONS_DATA = os.path.join(DATA_DIR, "sample_analytics.transactions.json")
# SQL script paths
SCRIPT_PATH = os.path.join(DB_DIR, "script.sql")
QUERY_SQL_PATH = os.path.join(DB_DIR, "query.sql")

SQL_PATH = os.path.join(ROOT_DIR, SCRIPT_PATH)
DB_NAME = os.path.join(DB_DIR, "dw_financial.db")

def connect_to_db(db_name):
    try:
        conn = sqlite3.connect(db_name)
        conn.execute("PRAGMA foreign_keys = ON;")
        logger.info(f"Successfully connected to database: {db_name}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def create_tables(conn):
    try:
        cursor = conn.cursor()
        with open(SQL_PATH, 'r', encoding='utf-8') as file:
            sql_script = file.read()
        cursor.executescript(sql_script)
        conn.commit()
        logger.info("Tables created successfully")
    except FileNotFoundError:
        logger.error(f"SQL script file not found: {SQL_PATH}")

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            logger.info(f"Loaded JSON file: {os.path.basename(file_path)}")
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return None

def insert_function(conn, query, data, desc):
    try:
        conn.executemany(query, data)
        conn.commit()
        logger.info(f"Inserted {len(data)} {desc}")
        return True
    except sqlite3.DatabaseError as e:
        logger.error(f"Database error inserting {desc}: {e}")
        conn.rollback()
        return False

def get_dimension_map(conn, table_name, key_col, id_col):
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT {key_col}, {id_col} FROM {table_name}")
        rows = cursor.fetchall()
        id_map = {row[0]: row[1] for row in rows}
        logger.debug(f"Map for {table_name} created with {len(id_map)} entries.")
        return id_map
    except (sqlite3.DatabaseError, Exception) as e:
        logger.error(f"Error fetching map for {table_name}: {e}")
        return {}

def format_timestamp(date_data):
    try:
        if not isinstance(date_data, dict) or '$date' not in date_data:
            return None

        value = date_data['$date']

        if isinstance(value, dict) and '$numberLong' in value:
            return datetime.fromtimestamp(int(value['$numberLong']) / 1000)

        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')

        return None
    except Exception as e:
        logger.warning(f"Timestamp formatting error: {date_data} - {str(e)}")
        return None

def transformation_dim_dates(conn, transactions_data):
    unique_dates = set()
    error_count = 0

    for account in transactions_data:
        for transaction in account['transactions']:
            formatted_date = format_timestamp(transaction['date'])
            if formatted_date:
                unique_dates.add(formatted_date.date())
            else:
                error_count += 1
                logger.debug(f"Could not transform date: {transaction['date']}")

    if error_count > 0:
        logger.warning(f"{error_count} dates could not be transformed")

    registered_dates = []
    for date in sorted(unique_dates):
        id_date = int(date.strftime('%Y%m%d'))

        registered_dates.append((id_date, date.isoformat(), date.year, date.month, date.day))

    insert_function(
        conn,
        """
        INSERT OR IGNORE INTO DIM_DATES (id_date, full_date, year, month, day) 
        VALUES (?, ?, ?, ?, ?)
        """,
        registered_dates,
        "unique dates into DIM_DATES"
    )

def transformation_dim_symbol(conn, transactions_data):
    unique_symbols = set()

    for account in transactions_data:
        for transaction in account['transactions']:
            symbol = transaction.get('symbol')
            if symbol:
                unique_symbols.add(symbol)
    
    registered_symbols = [(symbol,) for symbol in sorted(unique_symbols)]

    insert_function(
        conn,
        "INSERT OR IGNORE INTO DIM_SYMBOL (name_symbol) VALUES (?)",
        registered_symbols,
        "unique symbols into DIM_SYMBOL"
    )
    
    symbol_map = get_dimension_map(conn, "DIM_SYMBOL", "name_symbol", "ID_SYMBOL")
    return symbol_map

def transformation_dim_customers(conn, customers_data):
    registered_customers = []
    username_name_set = set()
    skipped_count = 0

    for customer in customers_data:
        username = customer.get('username')
        name = customer.get('name')
        username_name_key = f"{username}_{name}"

        if not username or not name:
            skipped_count += 1
            logger.debug(f"Skipping customer: missing username or name for {username_name_key}")
            continue
            
        if username_name_key in username_name_set:
            skipped_count += 1
            logger.debug(f"Skipping duplicate customer: {username_name_key}")
            continue
            
        username_name_set.add(username_name_key)

        birth_date = customer.get('birthdate')
        birth_date_iso = None
        if birth_date:
            formatted_birth_date = format_timestamp(birth_date) 
            if formatted_birth_date:
                birth_date_iso = formatted_birth_date.date().isoformat()
            else:
                logger.debug(f"Could not transform birthdate: {birth_date}")
        
        tier = None
        benefits = set()
        tier_details = customer.get('tier_and_details', {})

        for details in tier_details.values():
            if details.get('active', False):
                tier = details.get('tier')
                benefits.update(details.get('benefits', []))
        
        benefits_str = json.dumps(list(benefits)) if benefits else None

        registered_customers.append((name, username, username_name_key, birth_date_iso,  tier, benefits_str))

    insert_function(
        conn,
        """
        INSERT OR IGNORE INTO DIM_CUSTOMERS 
        (name_customer, username, customer_natural_key, birthdate, tier, benefits) 
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        registered_customers,
        "customers into DIM_CUSTOMERS"
    )
    logger.debug(f"Skipped {skipped_count} invalid/duplicate customers")

    customer_map = get_dimension_map(conn, "DIM_CUSTOMERS", "customer_natural_key", "ID_CUSTOMER")
    return customer_map

def relation_customers_and_accounts(customers_data):
    account_customer_map = {}
    for customer in customers_data:
        username = customer.get('username')
        name = customer.get('name')
        if username and name:
            customer_natural_key = f"{username}_{name}"
            for account_id in customer.get('accounts', []):
                account_customer_map[account_id] = customer_natural_key
    logger.debug(f"Created account-customer map with {len(account_customer_map)} entries")
    return account_customer_map

def transformation_dim_accounts(conn, accounts_data, account_customer_map, customer_map):
    registered_accounts = []
    skipped_count = 0

    for account in accounts_data:
        account_id = account.get('account_id')
        limit = account.get('limit', 0)
        products = account.get('products')
        products_str = json.dumps(products) if products else None
        
        customer_natural_key = account_customer_map.get(account_id)
        customer_id = customer_map.get(customer_natural_key) if customer_natural_key else None

        if customer_id is None:
            skipped_count += 1
            logger.debug(f"Skipping account {account_id}: customer not found")
            continue

        registered_accounts.append((account_id, customer_id, limit, products_str))

    insert_function(
        conn,
        """
        INSERT OR IGNORE INTO DIM_ACCOUNTS 
        (id_account, customer_id, limit_budget, products) 
        VALUES (?, ?, ?, ?)
        """,
        registered_accounts,
        f"accounts into DIM_ACCOUNTS (skipped {skipped_count} accounts without valid customer)"
    )

    account_map = get_dimension_map(conn, "DIM_ACCOUNTS", "id_account", "ID_ACCOUNT_UNIQUE")
    logger.debug(f"Account map created with {len(account_map)} entries")
    return account_map

def transformation_dim_type_transactions(conn, transactions_data):
    unique_transaction_types = set()

    for account in transactions_data:
        for transaction in account['transactions']:
            trans_code = transaction.get('transaction_code')
            if trans_code:
                unique_transaction_types.add(trans_code)

    registered_types = [(trans_type,) for trans_type in sorted(unique_transaction_types)]

    insert_function(
        conn,
        "INSERT OR IGNORE INTO DIM_TYPE_TRANSACTIONS (name_type_transacion) VALUES (?)",
        registered_types,
        "transaction types into DIM_TYPE_TRANSACTIONS"
    )

    type_transaction_map = get_dimension_map(conn, "DIM_TYPE_TRANSACTIONS", "name_type_transacion", "ID_TYPE_TRANSACTION")
    logger.debug(f"Transaction type map created with {len(type_transaction_map)} entries")
    return type_transaction_map

def transformation_fact_transactions(
    conn, transactions_data, account_customer_map, 
    account_map, customer_map, symbol_map, transaction_type_map
):
    fact_records = []
    skipped_count = 0

    for account_trans in transactions_data:
        account_id = account_trans['account_id']
        account_id_unique = account_map.get(account_id)
        customer_natural_key = account_customer_map.get(account_id)
        customer_id = customer_map.get(customer_natural_key) if customer_natural_key else None

        for trans in account_trans['transactions']:
            formatted_date = format_timestamp(trans['date'])
            if not formatted_date:
                skipped_count += 1
                logger.debug(f"Skipping transaction: invalid date {trans['date']}")
                continue
                
            date_id = int(formatted_date.strftime('%Y%m%d'))
            amount = float(trans['amount'])
            
            price = float(trans['price']) if trans.get('price') is not None else None
            total = float(trans['total']) if trans.get('total') is not None else None
            
            trans_code = trans['transaction_code']
            trans_type_id = transaction_type_map.get(trans_code)
            symbol_id = symbol_map.get(trans.get('symbol')) if trans.get('symbol') else None

            fact_records.append((
                date_id, customer_id, account_id_unique, trans_type_id, symbol_id,
                amount, price, total, trans_code, formatted_date.isoformat()
            ))
    
    insert_function(
        conn,
        """
        INSERT OR IGNORE INTO FACT_TRANSACTIONS 
        (date_id, customer_id, account_id, type_transaction_id, symbol_id, amount, price, total, transaction_code, transaction_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        fact_records,
        f"transaction records into FACT_TRANSACTIONS (skipped {skipped_count} invalid transactions)"
    )

def query_data(conn):
    cursor = conn.cursor()
    
    # First query: account limits statistics
    cursor.execute("SELECT limit_budget FROM DIM_ACCOUNTS WHERE limit_budget IS NOT NULL;")
    budgets = [row[0] for row in cursor.fetchall()]
    
    if budgets:
        avg = sum(budgets) / len(budgets)
        min_val = min(budgets)
        max_val = max(budgets)
        stddev = (sum((x - avg) ** 2 for x in budgets) / len(budgets)) ** 0.5
        
        logger.info("Account Limit Statistics:")
        logger.info(f"  Average: {avg:.2f}")
        logger.info(f"  Minimum: {min_val:.2f}")
        logger.info(f"  Maximum: {max_val:.2f}")
        logger.info(f"  Standard Deviation: {stddev:.2f}")
    else:
        logger.warning("No budget data found in DIM_ACCOUNTS")

    # Execute queries from SQL file
    try:
        with open(QUERY_SQL_PATH, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        queries = [q.strip() for q in sql_content.split(';') if q.strip()]
        logger.info(f"Executing {len(queries)} queries from {QUERY_SQL_PATH}")
        
        for i, query in enumerate(queries, 1):
            if not query:
                continue
                
            try:
                logger.debug(f"Executing query {i}: {query[:50]}...")
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    columns = [desc[0] for desc in cursor.description]
                    logger.info(f"\nQuery {i} Results ({len(results)} rows):")
                    logger.info(" | ".join(columns))
                    for row in results:
                        logger.info(" | ".join(str(value) for value in row))
                else:
                    logger.info(f"Query {i} returned no results")
            except sqlite3.OperationalError as e:
                logger.error(f"Error executing query {i}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error with query {i}: {e}")
                
    except FileNotFoundError:
        logger.error(f"SQL query file not found: {QUERY_SQL_PATH}")
    except Exception as e:
        logger.exception(f"Error processing query file: {e}")

def run_etl():
    logger.info("Starting ETL process")
    conn = None
    try:
        # Connect to database
        conn = connect_to_db(DB_NAME)
        create_tables(conn)

        # Load data
        logger.info("--- Extraction Phase ---")
        accounts_data = load_json_file(ACCOUNTS_DATA)
        customers_data = load_json_file(CUSTOMERS_DATA)
        transactions_data = load_json_file(TRANSACTIONS_DATA)
        
        if not all([accounts_data, customers_data, transactions_data]):
            logger.error("Essential data missing. Aborting ETL process")
            return
        
        logger.info("--- Transformation & Loading Phase ---")
        
        # Transform dimensions
        transformation_dim_dates(conn, transactions_data)
        symbol_map = transformation_dim_symbol(conn, transactions_data)
        customer_map = transformation_dim_customers(conn, customers_data)
        account_customer_map = relation_customers_and_accounts(customers_data)
        account_map = transformation_dim_accounts(conn, accounts_data, account_customer_map, customer_map)
        transaction_type_map = transformation_dim_type_transactions(conn, transactions_data)
        
        # Transform facts
        logger.info("--- Fact Table Processing ---")
        transformation_fact_transactions(
            conn, transactions_data, 
            account_customer_map, account_map, 
            customer_map, symbol_map, transaction_type_map
        )
        
        # Query results
        logger.info("--- Querying Phase ---")
        query_data(conn)
        
        logger.info("ETL process completed successfully")

    except sqlite3.DatabaseError as e:
        logger.error(f"Critical database error: {e}")
        if conn:
            conn.rollback()
            logger.info("Transaction rolled back")
    except Exception as e:
        logger.exception(f"Unexpected error in ETL process: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    run_etl()