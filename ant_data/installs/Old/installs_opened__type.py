"""
Installs Opened by Model and Open Type
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installs opened by model and Open type

- Create date:  2018-12-15
- Update date:  2018-12-26
- Version:      1.0

Notes:
============================
- v1.0: Initial version based on systems_opened
- v1.1: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TIME import TZ


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month'):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  s = Search(using=elastic, index=CONFIG['ES']['INSTALLS']) \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='install')
      ]
    )

  if start is not None:
    s = s.query('bool', filter=Q('range', opened={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', opened={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='opened', interval=interval, min_doc_count=1) \
      .bucket('models', 'terms', field='model', size=1000, min_doc_count=1) \
      .bucket('otypes', 'terms', field='open_type', size=1000, min_doc_count=1)

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  response = search(country, start=start, end=end, f=f, interval=interval)

  obj = {}
  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = {}
    for model in date.models.buckets:
      obj[date.key_as_string][model.key] = {}
      for otype in model.otypes.buckets:
        obj[date.key_as_string][model.key][otype.key] = otype.doc_count

  df = DataFrame.from_dict(
    {(i,j): obj[i][j] for i in obj.keys() for j in obj[i].keys()},
    orient='index', dtype='int64'
  )

  if df.empty:
    return df

  df.index = df.index.set_levels(df.index.levels[0].astype('datetime64'), level=0)
  df = df.sort_index().fillna(0).astype('int64')
  df.index = df.index.set_names('date', level=0)
  df.index = df.index.set_names('model', level=1)
  df['total'] = df.sum(axis=1)
  df = df.reset_index().set_index('date')

  return df

