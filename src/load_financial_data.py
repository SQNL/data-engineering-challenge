# load_financial_data.py

import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import zipfile
import shutil
from tqdm import tqdm


def connect_to_db():
    return psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST']
    )


def extract_financial_data(data, folder_name):
    bs_data = data.get('data', {}).get('bs', [])
    ic_data = data.get('data', {}).get('ic', [])
    cf_data = data.get('data', {}).get('cf', [])

    def find_value(items, concept):
        value = next((item['value'] for item in items if item['concept'] == concept), None)
        if value in ['N/A', '']:
            return None
        return value

    year, quarter = folder_name.split('.')

    return {
        'company_symbol': data.get('symbol')[:10],  # Limitar a 10 caracteres
        'date': datetime.strptime(data.get('endDate', ''), '%Y-%m-%d').date(),
        'report_type': quarter,
        'year': int(year),
        'total_revenue': find_value(ic_data, 'SalesRevenueNet'),
        'net_income': find_value(ic_data, 'NetIncomeLoss'),
        'revenues': find_value(ic_data, 'Revenues'),
        'cost_of_services': find_value(ic_data, 'CostOfServices'),

        'earnings_per_share_basic': find_value(ic_data, 'EarningsPerShareBasic'),
        'cost_of_revenue': find_value(ic_data, 'CostOfRevenue'), # ...
        'gross_profit': find_value(ic_data, 'GrossProfit'), # ...
        'operating_expenses': find_value(ic_data, 'OperatingExpenses'), # ...
        'operating_income': find_value(ic_data, 'OperatingIncomeLoss'), # ...
        'earnings_per_share': find_value(ic_data, 'EarningsPerShareBasic'), # ...
        'total_assets': find_value(bs_data, 'Assets'),
        'total_liabilities': find_value(bs_data, 'Liabilities'),
        'stock_holders_equity': find_value(bs_data, 'StockholdersEquity'),
        'cash_and_equivalents': find_value(bs_data, 'CashAndCashEquivalentsAtCarryingValue'),

        'receivables_net': find_value(bs_data, 'ReceivablesNetCurrent'), # ---
        'inventory': find_value(bs_data, 'InventoryNet'),
        'current_assets': find_value(bs_data, 'AssetsCurrent'),
        'current_debt': find_value(bs_data, 'DebtCurrent'),
        'long_term_debt': find_value(bs_data, 'LongTermDebtCurrent'), # ---

        'net_cash_from_investing_activities': find_value(cf_data,'NetCashProvidedByUsedInInvestingActivitiesContinuingOperations'),
        'net_cash_from_financing_activities': find_value(cf_data,'NetCashProvidedByUsedInFinancingActivitiesContinuingOperations'),
        'operating_cash_flow': find_value(cf_data, 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'),
        'capital_expenditures': find_value(cf_data, 'PaymentsToAcquireProductiveAssets'),
        'depreciation_and_amortization': find_value(cf_data, 'DepreciationAndAmortization')
    }


def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Obtener la lista de archivos en el ZIP
        file_list = zip_ref.namelist()

        # Crear la barra de progreso
        with tqdm(total=len(file_list), unit='file') as pbar:
            for file in file_list:
                zip_ref.extract(file, extract_to)
                pbar.update(1)  # Actualizar la barra de progreso


def load_json_files(conn, base_directory):
    json_files = []
    for root, dirs, files in os.walk(base_directory):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))

    total_files = len(json_files)

    with tqdm(total=total_files, unit='file') as pbar:
        for file_path in json_files:
            folder_name = os.path.basename(os.path.dirname(file_path))
            try:
                with open(file_path, 'r') as json_file:
                    try:
                        data = json.load(json_file)
                    except json.JSONDecodeError:
                        pbar.update(1)
                        continue

                financial_data = extract_financial_data(data, folder_name)

                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO financial_data 
                    (company_symbol,
                                date,
                                report_type,
                                year,
                                total_revenue,
                                net_income,
                                revenues,
                                cost_of_services,

                                earnings_per_share_basic,
                                cost_of_revenue,
                                gross_profit,
                                operating_expenses,
                                operating_income,
                                earnings_per_share,
                                total_assets,
                                total_liabilities,
                                stock_holders_equity,
                                cash_and_equivalents,

                                receivables_net,
                                inventory,
                                current_assets,
                                current_debt,
                                long_term_debt,

                                net_cash_from_investing_activities,
                                net_cash_from_financing_activities,
                                operating_cash_flow,
                                capital_expenditures,
                                depreciation_and_amortization)
                    VALUES (%(company_symbol)s, 
                                %(date)s, 
                                %(report_type)s, 
                                %(year)s, 
                                %(total_revenue)s, 
                                %(net_income)s, 
                                %(revenues)s, 
                                %(cost_of_services)s, 

                                %(earnings_per_share_basic)s, 
                                %(cost_of_revenue)s, 
                                %(gross_profit)s, 
                                %(operating_expenses)s, 
                                %(operating_income)s, 
                                %(earnings_per_share)s, 
                                %(total_assets)s, 
                                %(total_liabilities)s, 
                                %(stock_holders_equity)s, 
                                %(cash_and_equivalents)s, 

                                %(receivables_net)s, 
                                %(inventory)s, 
                                %(current_assets)s, 
                                %(current_debt)s, 

                                %(long_term_debt)s, 

                                %(net_cash_from_investing_activities)s, 
                                %(net_cash_from_financing_activities)s, 
                                %(operating_cash_flow)s, 
                                %(capital_expenditures)s, 
                                %(depreciation_and_amortization)s)
                    """, financial_data)
                conn.commit()
            except Exception as e:
                print(f"\nError al procesar {file_path}: {e}")
                conn.rollback()
                return False  # Detiene el proceso en caso de error
            pbar.update(1)

    return True


if __name__ == "__main__":
    conn = None
    try:
        data_directory = '/app/data'
        zip_file = os.path.join(data_directory, 'financial_data.zip')
        extract_directory = os.path.join(data_directory, 'extracted')

        if not os.path.exists(extract_directory):
            print("Descomprimiendo archivo ZIP...")
            unzip_file(zip_file, extract_directory)
        else:
            print("La carpeta extraída ya existe. Saltando el paso de descompresión.")

        conn = connect_to_db()
        print("Cargando archivos json a BD...")
        success = load_json_files(conn, extract_directory)

        if success:
            print("\nTodos los archivos se cargaron exitosamente.")
        else:
            print("\nLa carga de datos se detuvo debido a un error.")

    except Exception as e:
        print(f"Error durante la carga de datos: {e}")
    finally:
        if conn:
            conn.close()
