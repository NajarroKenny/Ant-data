"""
Clients Closed
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on clients closed

- Create date:  2018-12-04
- Update date:  2018-12-07
- Version:      1.1

Notes:
==========================        
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_closed
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='client'),
        Q('range', closed={'lte': 'now'})
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'dates', 'date_histogram', field='closed', interval=interval, 
    min_doc_count=0
  )

  return s[:0].execute()


def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.dates.buckets]
  obj = {x: { 0 } for x in dates}

  for date in response.aggregations.dates.buckets: 
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['closed']
  )

  if df.empty:
        return df
  
  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df

