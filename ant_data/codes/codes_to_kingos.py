
"""
Credits by Plan
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits by plan

- Create date:  2018-11-30
- Update date:  2018-12-26
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
- v1.2: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ..static.FINANCE import IVA


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index=CONFIG['ES']['CODES']) \
    .query('term', doctype='code') \
    .query('bool', filter=Q('term', country=country))

  if start is not None:
    s = s.query('bool', filter=Q('range', datetime={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', datetime={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'dates', 'date_histogram', field='datetime', interval=interval,
    min_doc_count=1
  )
  s.aggs['dates'].bucket('parents', 'filter', filter=Q('exists', field='to.person_id'))
  s.aggs['dates'].bucket('orphan', 'filter', filter=~Q('exists', field='to.person_id'))

  s.aggs['dates']['parents'].bucket('paid', 'filter', filter=Q('term', sale=True)) \
    .bucket('kingos', 'cardinality', field='to.system_id')
  s.aggs['dates']['parents'].bucket('free', 'filter', filter=Q('term', sale=False)) \
    .bucket('kingos', 'cardinality', field='to.system_id')
  s.aggs['dates']['orphan'].bucket('paid', 'filter', filter=Q('term', sale=True)) \
    .bucket('kingos', 'cardinality', field='to.system_id')
  s.aggs['dates']['orphan'].bucket('free', 'filter', filter=Q('term', sale=False)) \
    .bucket('kingos', 'cardinality', field='to.system_id')

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
  response = search(country, start=start, end=end, f=f, interval=interval)

  obj = {}

  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = {
      'parents.paid': date.parents.paid.kingos.value,
      'parents.free': date.parents.free.kingos.value,
      'orphan.paid': date.orphan.paid.kingos.value,
      'orphan.free': date.orphan.free.kingos.value
    }

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return DataFrame(columns=[''])

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  df = df.fillna(0).astype('double')
  df['total'] = df.sum(axis=1)

  return df
