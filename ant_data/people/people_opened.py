"""
People Opened
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on people opened

- Create date:  2018-12-04
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
- v1.2: Updated with standard based on v1.1 of systems_closed
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def search(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
    .query('term', country=country)

  if start is not None:
    s = s.query('bool', filter=Q('range', opened={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', opened={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='opened', interval=interval, min_doc_count=1)

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
  response = search(country, start=start, end=end, f=f, interval=interval)

  obj = {}
  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['opened']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df

