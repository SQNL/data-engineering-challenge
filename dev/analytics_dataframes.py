# analytics_dataframes.py

import pandas as pd
from sqlalchemy import create_engine
import os, sys, io

def get_db_connection_string():
    return f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

def load_table_data(query):
    engine = create_engine(get_db_connection_string())
    return pd.read_sql(query, engine)

# Cargar datos necesarios
def load_data():
    companies_df = load_table_data("SELECT * FROM companies")
    financial_periods_df = load_table_data("SELECT * FROM financial_periods where year = 2020")

    # KPI 1: Profit Margin
    profit_margin_df = pd.merge(companies_df, income_statements_df, left_on='id', right_on='company_id')
    profit_margin_df = pd.merge(profit_margin_df, financial_periods_df, left_on='period_id', right_on='id')
    profit_margin_df['profit_margin'] = profit_margin_df.apply(
        lambda row: row['net_income'] / row['revenues'] if row['revenues'] != 0 else None,
        axis=1
    )
    profit_margin_df = profit_margin_df[['company_id', 'period_id', 'profit_margin']]

    # KPI 2: Debt-to-Equity Ratio


    # KPI 3: Return on Assets (ROA)



    # Fusionar los tres KPIs en un solo DataFrame



    return kpis_df



def load_kpis_to_db(df):
    engine = create_engine(get_db_connection_string())
    
    # Cargar datos directamente en la tabla financial_kpis
    



if __name__ == "__main__":
    df = load_data()
    load_kpis_to_db(df)

# Llamar a la función para cargar los KPIs
# load_kpis_to_db(kpis_df)

