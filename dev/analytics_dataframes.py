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
    balance_sheets_df = load_table_data("SELECT * FROM balance_sheets")
    income_statements_df = load_table_data("SELECT * FROM income_statements")

    # KPI 1: Profit Margin
    profit_margin_df = pd.merge(companies_df, income_statements_df, left_on='id', right_on='company_id')
    profit_margin_df = pd.merge(profit_margin_df, financial_periods_df, left_on='period_id', right_on='id')
    profit_margin_df['profit_margin'] = profit_margin_df.apply(
        lambda row: row['net_income'] / row['revenues'] if row['revenues'] != 0 else None,
        axis=1
    )
    profit_margin_df = profit_margin_df[['company_id', 'period_id', 'profit_margin']]

    # KPI 2: Debt-to-Equity Ratio
    debt_equity_df = pd.merge(companies_df, balance_sheets_df, left_on='id', right_on='company_id')
    debt_equity_df = pd.merge(debt_equity_df, financial_periods_df, left_on='period_id', right_on='id')
    debt_equity_df['debt_to_equity_ratio'] = debt_equity_df.apply(
        lambda row: row['total_liabilities'] / row['stock_holders_equity'] 
        if row['stock_holders_equity'] != 0 else None,
        axis=1
    )
    debt_equity_df = debt_equity_df[['company_id', 'period_id', 'debt_to_equity_ratio']]

    # KPI 3: Return on Assets (ROA)
    roa_df = pd.merge(companies_df, balance_sheets_df, left_on='id', right_on='company_id')
    roa_df = pd.merge(roa_df, financial_periods_df, left_on='period_id', right_on='id')
    roa_df = pd.merge(roa_df, income_statements_df, on=['company_id', 'period_id'])
    roa_df['return_on_assets'] = roa_df.apply(
        lambda row: row['net_income'] / row['total_assets'] if row['total_assets'] > 0 else None,
        axis=1
    )
    roa_df = roa_df[['company_id', 'period_id', 'return_on_assets']]

    # Fusionar los tres KPIs en un solo DataFrame
    kpis_df = pd.merge(profit_margin_df, debt_equity_df, on=['company_id', 'period_id'], how='outer')
    kpis_df = pd.merge(kpis_df, roa_df, on=['company_id', 'period_id'], how='outer')
    return kpis_df



def load_kpis_to_db(df):
    engine = create_engine(get_db_connection_string())
    
    # Cargar datos directamente en la tabla financial_kpis
    df.to_sql('financial_kpis', engine, if_exists='append', index=False)
    



if __name__ == "__main__":
    df = load_data()
    load_kpis_to_db(df)

# Llamar a la función para cargar los KPIs
# load_kpis_to_db(kpis_df)

