import sqlite3
import pandas as pd
import os

SQL_PATH = os.path.join("db", "script.sql")

conn = sqlite3.connect('dw_financial.db')
cursor = conn.cursor()

# First we need to create the tables in the database, so we use the SQL script in db folder
with open(SQL_PATH, 'r', encoding='utf-8') as file:
    sql_script = file.read()

try:
    cursor.executescript(sql_script)
    conn.commit()

    print("Tables was created successfully.")

except sqlite3.Error as e:
    print(f"An error occurred, this was: {e}")
    if conn:
        conn.rollback()
        conn.close()
        print("Changes have been rolled back due to the error. Check the SQL script for issues.")

finally:
    if conn:
        conn.close()
        print("Connection to the database has been closed.")
