# validate_table.py

import os
import psycopg2

def connect_to_db():
    return psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST']
    )

def check_table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name,))
        return cur.fetchone()[0]

def validate_table_structure(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_name = %s;
        """, (table_name,))
        columns = cur.fetchall()
        
        expected_columns = {
            'id': 'integer',
            'company_symbol': 'character varying',
            'date': 'date',
            'report_type': 'character varying',
            'year_': 'numeric',
            'total_revenue': 'numeric',
            'net_income': 'numeric',
            'total_assets': 'numeric',
            'total_liabilities': 'numeric',
            'cash_and_equivalents': 'numeric',
            'data_json': 'jsonb'
        }
        
        for col, type_ in columns:
            if col in expected_columns:
                if expected_columns[col] != type_:
                    print(f"Advertencia: La columna {col} es de tipo {type_}, se esperaba {expected_columns[col]}")
            else:
                print(f"Columna adicional encontrada: {col}")
        
        for col in expected_columns:
            if col not in [c[0] for c in columns]:
                print(f"Falta la columna esperada: {col}")

if __name__ == "__main__":
    conn = connect_to_db()
    table_name = 'financial_data'
    
    if check_table_exists(conn, table_name):
        print(f"La tabla '{table_name}' existe.")
        validate_table_structure(conn, table_name)
    else:
        print(f"La tabla '{table_name}' no existe.")
    
    conn.close()