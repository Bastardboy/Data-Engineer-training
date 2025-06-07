import sqlite3
import os

QUERY_SQL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "db", "query.sql"))


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
