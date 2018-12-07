"""
Systems Closed by Model
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on systems closed by model

- Create date:  2018-12-06
- Update date:  2018-12-06
- Version:      1.1

Notes:
==========================        
- v1.0: Uses min_doc_count parameter and pre-populates de obj dictionary 
        with all date x model combinations to guarantee dates are not sparse.
- v1.1: Put filter on closed date < now. Added get method to df_open_now
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo'), 
        Q('range', closed={'lte': 'now'})
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
      'dates', 'date_histogram', field='closed', interval=interval, 
      min_doc_count=0
    ).bucket(
      'models', 'terms', field='model', exclude=['Kingo Shopkeeper', 'Ant Mobile'],
      min_doc_count=0
    )

  return s[:0].execute()


def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.dates.buckets]
  models = list(
    {y.key for x in response.aggs.dates.buckets for y in x.models.buckets}
  )

  obj = {(x, y): { 'closed': 0 } for x in dates for y in models}

  for date in response.aggs.dates.buckets: 
    for model in date.models.buckets:
      obj[(date.key_as_string, model.key)] = { 'closed': model.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['closed']
  )
  
  if df.empty:
    return df

  df = df.rename_axis(['date', 'model'])
  idx = MultiIndex(
    levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
    labels=df.index.labels, names=['date', 'model']
  )
  df = DataFrame(df.values, index=idx, columns=['closed']).sort_index().reset_index()
  df = df.set_index('date').sort_values(['model', 'date'])

  return df

