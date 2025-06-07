from utils.transform_dates import formate_timestamp
import sqlite3
import json

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