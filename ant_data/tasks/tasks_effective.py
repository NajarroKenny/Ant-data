"""
Tasks effective
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task effectiveness.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.tasks import tasks_workflow_list as twl


WORKFLOW_LIST=[
  'active-code', 'install', 'pickup', 'register', 'sale', 'swap',
  'visit-install'
  ]

def df(country, f=None, interval='month'):
    if country not in COUNTRY_LIST:
      raise Exception(f'{country} is not a valid country')

    df = twl.df(country, WORKFLOW_LIST, f, interval)
    df = df.rename(columns={ 'should': 'effective', 'must_not': 'not_effective' })

    return df