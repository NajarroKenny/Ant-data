from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series

from .days_installed__model_month import df as installed_df
from .days_active__model_month import df as active_df

def df(q = None):
    df = active_df(q).div(installed_df(q))
    df = df.drop(['days_in_month'], axis=1)
    return df