import pyodbc as odbc
import pandas as pd
from datetime import datetime
import json
from pwbi_flow.secrets.get_token import get_secret
from pwbi_flow.resources.config import *
from urllib.parse import quote_plus
from sqlalchemy import DATETIME, create_engine,event,text
from io import StringIO
import boto3
import time
import slack


def slackAlerta(msg):
    token = (get_secret(SLACK_TOKEN))
    client = slack.WebClient(token=token)
    client.chat_postMessage(channel='alertas_engenharia',text=msg)


def pushToDataLake(schema,name,data):
    bucket = 'lkc-data-lake' 
    csv_buffer = StringIO()
    data.to_csv(csv_buffer,encoding="utf8",index=False)
    s3_resource = boto3.resource('s3',
                                    aws_access_key_id=json.loads(get_secret("prod/secret"))['Access Key Id'],
                                    aws_secret_access_key=json.loads(get_secret("prod/secret"))['Secret Access Key'])
    s3_resource.Object(bucket,
                    DATA_SOURCE +  schema + str(time.strftime('%Y/%m/%d/')) + name).put(Body=csv_buffer.getvalue())
    print("Data uploaded sucessfuly in Data Lake -> " + name)
    print(time.ctime())


def sqlConnector():
    server = json.loads(get_secret("prod/BancoSQLServer"))['host']
    database = json.loads(get_secret("prod/BancoSQLServer"))['dbname']
    username = json.loads(get_secret("prod/BancoSQLServer"))['username']
    password = json.loads(get_secret("prod/BancoSQLServer"))['password']
    conn = ('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    quoted = quote_plus(conn)
    new_con = f'mssql+pyodbc:///?odbc_connect={quoted}'

    return create_engine(new_con,fast_executemany=True, use_insertmanyvalues=False)


def postgresConn():
    secrets = json.loads(get_secret(AURORA))
    conn = psycopg2.connect(host=secrets['host'], dbname=secrets['dbname'], user=secrets['username'], password=secrets['password'])
    return conn

def load_data_postgresql(table_name: str, df: pd.DataFrame):
  
    con = postgresConn()
    cur = con.cursor()
    try:
        cur.execute("TRUNCATE TABLE 'pbi'.{table_name}")
        con.commit()
        print(f'dedudplicated {schema}.{table_name}')
    except (Exception, psycopg2.DatabaseError) as error_content:
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Error truncating POSTGRESQL - {'pbi'}.{table_name}
        ETL: lkc-pwbi-dataset-flow
        {time_now}
        """)
        con.rollback()
        cur.close()
        return 1
    cur.close()
    
    try:
        secrets = json.loads(get_secret(AURORA))
        conn_string = 'postgresql://' + str(secrets['username']) + ':' + str(secrets['password']) + '@' + str(
            secrets['host']) + ':5432/' + str(secrets['dbname'])
        db = create_engine(conn_string)
        table_name = 'salesforce_leads'
        schema = 'public'
        df.to_sql(table_name, con=db, schema=schema, index=False, if_exists='append', method='multi',
                    chunksize=((64000 // len(df.columns)) - 1))
        print(f'inserted into {schema}.{table_name}')
    except Exception as e:
        error_content = e
        print(e)

    if error_content is not None:
        slackAlerta(f"""
        URGENTE VERIFICAR:
        ERRO: Error inserting POSTGRESQL - 'pbi'.{table_name}
        ETL: lkc-pwbi-dataset-flow
        {time_now}
        """)