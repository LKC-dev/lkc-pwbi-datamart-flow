import pandas as pd
from datetime import datetime
from pwbi_flow.ETL.extract import *
from pwbi_flow.ETL.transform import *
from pwbi_flow.ETL.load import *
from pwbi_flow.logs import logger

logger = logger.logger_configuration()

def run():

    table_name = 'your_table'

    data = extract()

    df = transform(data)

    load_data_postgresql(table_name, df)

    pushToDataLake(table_name, df, 'pwbi')

try:
    run()
except Exception as e:
    logger.error(f"An error occurred: {e}")
    slackAlerta(f"""
            URGENTE VERIFICAR:
            ERRO: Execution error
            ETL: lkc-pwbi-datamart-flow
            {datetime.now()}
            """)
