# load_analytics_tables.py

import pandas as pd
from sqlalchemy import create_engine
import os

def get_db_connection_string():
    return f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

def load_table(engine, source_query, target_table):
    print(f"Cargando datos en la tabla {target_table}...")
    df = pd.read_sql(source_query, engine)
    df.to_sql(target_table, engine, if_exists='append', index=False)
    print(f"Datos cargados en {target_table}: {len(df)} filas")

def migrate_data():
    engine = create_engine(get_db_connection_string())
    
    tables_to_load = [
        ("SELECT DISTINCT company_symbol as symbol, company_symbol as company_name FROM financial_data", "companies"),
        
        ("SELECT DISTINCT year, report_type as quarter FROM financial_data", "financial_periods"),
        
        ("""SELECT 
            c.id as company_id, 
            fp.id as period_id,
            COALESCE(fd.cash_and_equivalents, 0) as cash_and_equivalents, 
            COALESCE(fd.receivables_net, 0) as receivables_net, 
            COALESCE(fd.inventory, 0) as inventory, 
            COALESCE(fd.current_assets, 0) as current_assets, 
            COALESCE(fd.property_plant_equipment_net, 0) as property_plant_equipment_net, 
            COALESCE(fd.total_assets, 0) as total_assets, 
            COALESCE(fd.current_debt, 0) as current_debt, 
            COALESCE(fd.current_liabilities, 0) as current_liabilities,
            COALESCE(fd.stock_holders_equity, 0) as stock_holders_equity,
            COALESCE(fd.long_term_debt, 0) as long_term_debt, 
            COALESCE(fd.total_liabilities, 0) as total_liabilities 
        FROM financial_data fd
        JOIN companies c ON fd.company_symbol = c.symbol
        JOIN financial_periods fp ON fd.year = fp.year AND fd.report_type = fp.quarter""", "balance_sheets"),
        
        ("""SELECT 
            c.id as company_id,
            fp.id as period_id,
            COALESCE(fd.total_revenue, 0) as total_revenue, 
            COALESCE(fd.cost_of_revenue, 0) as cost_of_revenue, 
            COALESCE(fd.gross_profit, 0) as gross_profit, 
            COALESCE(fd.operating_expenses, 0) as operating_expenses, 
            COALESCE(fd.operating_income, 0) as operating_income, 
            COALESCE(fd.net_income, 0) as net_income, 
            COALESCE(fd.revenues, 0) as revenues, 
            COALESCE(fd.cost_of_services, 0) as cost_of_services, 
            COALESCE(fd.operating_income_loss, 0) as operating_income_loss, 
            COALESCE(fd.earnings_per_share_basic, 0) as earnings_per_share_basic, 
            COALESCE(fd.earnings_per_share, 0) as earnings_per_share 
        FROM financial_data fd
        JOIN companies c ON fd.company_symbol = c.symbol
        JOIN financial_periods fp ON fd.year = fp.year AND fd.report_type = fp.quarter""", "income_statements"),
        
        ("""SELECT 
            c.id as company_id,
            fp.id as period_id,
            COALESCE(fd.operating_cash_flow, 0) as operating_cash_flow, 
            COALESCE(fd.capital_expenditures, 0) as capital_expenditures, 
            COALESCE(fd.depreciation_and_amortization, 0) as depreciation_and_amortization, 
            COALESCE(fd.net_cash_from_operating_activities, 0) as net_cash_from_operating_activities, 
            COALESCE(fd.net_cash_from_investing_activities, 0) as net_cash_from_investing_activities, 
            COALESCE(fd.net_cash_from_financing_activities, 0) as net_cash_from_financing_activities  
        FROM financial_data fd
        JOIN companies c ON fd.company_symbol = c.symbol
        JOIN financial_periods fp ON fd.year = fp.year AND fd.report_type = fp.quarter""", "cash_flows")
    ]
        

    for query, table in tables_to_load:
        load_table(engine, query, table)
    
    print("Migración de datos completada.")

if __name__ == "__main__":
    migrate_data()