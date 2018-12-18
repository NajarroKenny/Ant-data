"""
Systems Closed by Model
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installs closed by model

- Create date:  2018-12-07
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Initial version based on systems_closed
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic


def search(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index='installs') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='install')
      ]
    )

  if start is not None:
    s = s.query('bool', filter=Q('range', closed={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', closed={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='closed', interval=interval, min_doc_count=1) \
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

