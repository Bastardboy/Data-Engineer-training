import sqlite3

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
