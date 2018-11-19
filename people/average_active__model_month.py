from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series

from .days_active__model_month import df as _df

def df(q = None):
    df = _df(q)
    df = df.div(df['days_in_month'], axis=0)
    df = df.drop(['days_in_month'], axis=1)
    return df

if __name__ == '__main__':
    print('Average installed by Model and Month')
    print(df())