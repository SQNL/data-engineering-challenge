# count_rows.py

import os
import psycopg2

def connect_to_db():
    return psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST']
    )

def count_rows():
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM financial_data")
            count = cur.fetchone()[0]
            print(f"Número total de filas en financial_data: {count}")
    except Exception as e:
        print(f"Error al contar filas: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    count_rows()