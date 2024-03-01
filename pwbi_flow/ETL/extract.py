import pandas as pd
import warnings
import pyodbc
import json
from datetime import datetime
from pwbi_flow.utils.sqlServerConnector import *
from pwbi_flow.resources.config import PWBI_SECRET


def extract():
    server = json.loads(get_secret(PWBI_SECRET))['fabric_azure_datamart_sql']
    database = 'master'
    username = json.loads(get_secret(PWBI_SECRET))['user']
    password = json.loads(get_secret(PWBI_SECRET))['password']
    driver = '{ODBC Driver 17 for SQL Server}'
    authentication_type = 'ActiveDirectoryPassword'

    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication={authentication_type};'
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        query = 'select * from central_projetos'

        rows = pd.read_sql(query, connection)

    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")

    finally:
        if 'connection' in locals():
            connection.close()

        return rows