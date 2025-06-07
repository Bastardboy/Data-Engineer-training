import json
import sqlite3


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
