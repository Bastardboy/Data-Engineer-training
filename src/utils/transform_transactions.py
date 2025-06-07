from utils.transform_dates import formate_timestamp
import sqlite3

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
            