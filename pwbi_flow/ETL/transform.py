from datetime import datetime
import pandas as pd


def transform(data):

    df = pd.DataFrame(data=data)

    date_columns = ['column1', 
                    'column2', 
                    'column3'
                    ]

    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    df['inserted_at'] = datetime.now()
    df = df.where(pd.notnull(df), None)

    df.to_csv('df.csv')

    return df
