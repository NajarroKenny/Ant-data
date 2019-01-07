"""
Installs Opened by Model
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installs opened by model

- Create date:  2018-12-07
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version based on systems_opened
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month'):
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
      .bucket('models', 'terms', field='model')

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
  response = search(country, start=start, end=end, f=f, interval=interval)

  obj = {}
  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = {}
    for model in date.models.buckets:
      obj[date.key_as_string][model.key] = model.doc_count

  df = DataFrame.from_dict(obj, orient='index', dtype='int64')

  if df.empty:
    return df

  df.index.name = 'date'
  df.index = df.index.astype('datetime64')
  df = df.sort_index().fillna(0).astype('int64')
  df['total'] = df.sum(axis=1)

  return df

