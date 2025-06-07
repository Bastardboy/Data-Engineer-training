from datetime import datetime
import sqlite3

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

        