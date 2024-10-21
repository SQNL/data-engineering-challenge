# list_tables.py

import os
from sqlalchemy import create_engine, MetaData

def get_db_connection_string():
    return f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

def list_tables():
    # Create a connection SQLAlchemy engine
    engine = create_engine(get_db_connection_string())

    # Create a MetaData object
    metadata = MetaData()

    # Get all tables reflected in the metadata
    metadata.reflect(engine)

    # List all table names
    print("Tables in the database:")
    for table in metadata.tables.values():
        print(table.name)

if __name__ == "__main__":
    list_tables()