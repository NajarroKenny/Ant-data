"""
Tasks effective
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task effectiveness.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.tasks import tasks__action_list as tal


action_list=['attach-kingo-new', 'attach-kingo-swap', 'attach-kingo-pickup',
               'receipt-week', 'receipt-fortnight', 'receipt-month', 'receipt-quarter', 'receipt-semester', 'receipt-year']

def df(country, f=None, interval='month'):
    return tal.df(country, action_list, f, interval)
