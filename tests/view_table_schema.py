# view_table_schema.py

import os
import psycopg2

def connect_to_db():
    return psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST']
    )

def view_table_schema(table_name):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            
            print(f"Esquema de la tabla '{table_name}':")
            for column in columns:
                print(f"- {column[0]}: {column[1]}")
    except Exception as e:
        print(f"Error al obtener el esquema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    view_table_schema('financial_data')