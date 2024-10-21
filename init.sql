-- init.sql
-- Tabla stage para cargar datos en tablas de analytics
CREATE TABLE IF NOT EXISTS financial_data (
    id SERIAL PRIMARY KEY,
    company_symbol VARCHAR(100),
    date DATE,
    report_type VARCHAR(50),
    year INTEGER,
    total_revenue NUMERIC,
    net_income NUMERIC,
    revenues NUMERIC, 
    cost_of_services NUMERIC, 
    operating_income_loss NUMERIC, 
    earnings_per_share_basic NUMERIC, 
    total_assets NUMERIC,
    total_liabilities NUMERIC,
    stock_holders_equity NUMERIC, 
    cash_and_equivalents NUMERIC,
    net_cash_from_investing_activities NUMERIC,
    net_cash_from_financing_activities NUMERIC,
    operating_cash_flow NUMERIC,
    capital_expenditures NUMERIC,
    depreciation_and_amortization NUMERIC,
    property_plant_equipment_net NUMERIC,
    receivables_net NUMERIC,
    inventory NUMERIC,
    current_assets NUMERIC,
    current_debt NUMERIC,
    long_term_debt NUMERIC,
    earnings_per_share NUMERIC,
    gross_profit NUMERIC,
    operating_expenses NUMERIC,
    operating_income NUMERIC,
    cost_of_revenue NUMERIC
);

-- Tabla principal de compañías
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE,
    company_name VARCHAR(255)
);

-- Tabla de períodos financieros
CREATE TABLE financial_periods (
    id SERIAL PRIMARY KEY,
    year INTEGER,
    quarter VARCHAR(4),
    start_date DATE,
    end_date DATE,
    UNIQUE (year, quarter)
);

-- Tabla de balance general (Balance Sheet)
CREATE TABLE balance_sheets (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_id INTEGER REFERENCES financial_periods(id),
    cash_and_equivalents NUMERIC,
    receivables_net NUMERIC,
    inventory NUMERIC,
    current_assets NUMERIC,
    property_plant_equipment_net NUMERIC,
    total_assets NUMERIC,
    current_debt NUMERIC,
    stock_holders_equity NUMERIC, 
    long_term_debt NUMERIC,
    total_liabilities NUMERIC
);

-- Tabla de estados de resultados (Income Statement)
CREATE TABLE income_statements (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_id INTEGER REFERENCES financial_periods(id),
    total_revenue NUMERIC,
    cost_of_revenue NUMERIC,
    gross_profit NUMERIC,
    operating_expenses NUMERIC,
    operating_income NUMERIC,
    net_income NUMERIC,
    revenues NUMERIC, 
    cost_of_services NUMERIC, 
    operating_income_loss NUMERIC, 
    earnings_per_share_basic NUMERIC, 
    earnings_per_share NUMERIC
);

-- Tabla de flujos de efectivo (Cash Flow)
CREATE TABLE cash_flows (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    period_id INTEGER REFERENCES financial_periods(id),
    operating_cash_flow NUMERIC,
    capital_expenditures NUMERIC,
    depreciation_and_amortization NUMERIC,
    net_cash_from_investing_activities NUMERIC,
    net_cash_from_financing_activities NUMERIC
);

-- Tabla de KPIs
CREATE TABLE financial_kpis (
    id SERIAL,
    company_id INTEGER REFERENCES companies(id),
    period_id INTEGER REFERENCES financial_periods(id),
    profit_margin NUMERIC,
    debt_to_equity_ratio NUMERIC,
    return_on_assets NUMERIC
);