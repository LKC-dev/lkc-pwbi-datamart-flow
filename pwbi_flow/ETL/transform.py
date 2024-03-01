from datetime import datetime
import pandas as pd


def transform(data):

    df = pd.DataFrame(data=data)

    date_columns = ['data_fim_movimentacao', 
                    'data_criacao', 
                    'data_assinatura', 
                    'data_fim_projeto',
                    'data_inicio_projeto',
                    'data_ultimo_pagamento',
                    'data_criacao_ultimo_variavel',
                    'data_ultima_resposta_pesquisa',
                    'data_atribuicao_trust',
                    'data_ultimo_roi'
                    ]

    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    df['inserted_at'] = datetime.now()
    df = df.where(pd.notnull(df), None)

    df.to_csv('df.csv')

    return df
