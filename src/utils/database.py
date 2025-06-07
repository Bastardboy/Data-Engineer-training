import sqlite3
import os 

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(ROOT_DIR, "db")
SQL_PATH = os.path.join(DB_DIR, "script.sql")
DB_NAME = "dw_financial.db"
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
