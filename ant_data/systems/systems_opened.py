"""
Systems Opened by Model
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on systems opened by model

- Create date:  2018-12-06
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Uses min_doc_count parameter and pre-populates de obj dictionary
        with all date x model combinations to guarantee dates are not sparse.
- v1.1: Put filter on closed date < now. Added get method to df_open_now
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country)
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='opened', interval=interval) \
      .bucket('models', 'terms', field='model')

  return s[:0].execute()


def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

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

