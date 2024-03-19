import pandas as pd
import warnings
import pyodbc
import json
from datetime import datetime
from pwbi_flow.utils.sqlServerConnector import *


def extract():
    server = ###YOUR_SERVER URL
    database = 'master'
    username = ###YOUR_USERNAME
    password = ###YOUR_PASSWORD
    driver = '{ODBC Driver 17 for SQL Server}'
    authentication_type = 'ActiveDirectoryPassword'

    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Authentication={authentication_type};'
    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        query = 'select * from you_table'

        rows = pd.read_sql(query, connection)

    except pyodbc.Error as e:
        print(f"Error connecting to SQL Server: {e}")

    finally:
        if 'connection' in locals():
            connection.close()

        return rows