import json
import time
from datetime import datetime
from io import StringIO
from urllib.parse import quote_plus
import boto3
import pandas as pd
import pyodbc as odbc
import slack
import psycopg2
from sqlalchemy import create_engine, text
from pwbi_flow.secrets.get_token import get_secret
from pwbi_flow.resources.config import *

def pushToDataLake(name, data, schema):
    data.to_csv(name, encoding="utf8", index=False)
    bucket = 'lkc-data-lake' 
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, encoding="utf8", index=False)
    s3_resource = boto3.resource('s3',
                                aws_access_key_id=json.loads(get_secret("prod/secret"))['Access Key Id'],
                                aws_secret_access_key=json.loads(get_secret("prod/secret"))['Secret Access Key']
                                )
    s3_resource.Object(bucket, 
                       DATA_SOURCE + schema + str(time.strftime('%Y/%m/%d/')) + name).put(Body=csv_buffer.getvalue())
    print("Data uploaded sucessfuly in Data Lake -> " + name)
    print(time.ctime())

def slackAlerta(msg):
    token = (get_secret("prod/Slack"))
    client = slack.WebClient(token=token)
    client.chat_postMessage(channel='alertas_engenharia', text=msg)

def postgresConn():
    secrets = json.loads(get_secret(AURORA))
    conn = psycopg2.connect(host=secrets['host'], dbname=secrets['dbname'], user=secrets['username'], password=secrets['password'])
    return conn

def load_data_postgresql(table_name: str, df: pd.DataFrame):
    for i in range(3):
        schema = 'public'            
        con = postgresConn()
        cur = con.cursor()
        try:
            cur.execute(f'TRUNCATE TABLE {schema}.{table_name}')
            con.commit()
            print(f'Truncated {schema}.{table_name}')
        except (Exception, psycopg2.DatabaseError) as error_content:
            slackAlerta(f"""
                            URGENTE VERIFICAR:
                            ERRO: Error Truncating POSTGRESQL - {schema}.{table_name}
                             ETL: lkc-datamart-aurora-flow
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
            df.to_sql(table_name, con=db, schema=schema, index=False, if_exists='append', method='multi',
                        chunksize=((64000 // len(df.columns)) - 1))
            print(f'inserted into {schema}.{table_name}')
            break
        except Exception as e:
            error_content = e
            date = datetime.now()
            print(e)

        if error_content is not None:
            slackAlerta(f"""
                URGENTE VERIFICAR:
                ERRO: Error inserting POSTGRESQL - {schema}.{table_name}
                ETL: lkc-datamart-aurora-flow
            """)