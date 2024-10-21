# view_data_simple.py

import os
import pandas as pd
from sqlalchemy import create_engine

def get_db_connection_string():
    return f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

def view_data_head():
    # Crear una conexión SQLAlchemy
    engine = create_engine(get_db_connection_string())

    # Cargar datos en un DataFrame
    query = "SELECT * FROM income_statements LIMIT 5"
    df = pd.read_sql(query, engine)
    print(df) 

if __name__ == "__main__":
    view_data_head()