import sqlite3

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
