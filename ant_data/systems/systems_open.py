"""
Systems Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on systems open by model. The 'open' count is done
in five different ways: 'start', 'end', 'average', 'distinct', and 'weighted'

- Create date:  2018-12-06
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Uses min_doc_count parameter and pre-populates de obj dictionary
        with all date x model combinations to guarantee dates are not sparse.
- v1.1: Fixed bug when key didn't exit in df_open_now
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, MultiIndex, offsets, Series, Timestamp

from ant_data import elastic
from ant_data.systems import systems_closed, systems_opened
from ant_data.shared import open_df


def search_open_now(country, f=None):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country),
        Q('term', open=True)
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('models', 'terms', field='model')

  return s[:0].execute()


def search_distinct(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query('term', country=country)

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'models', 'terms', field='model'
  ).bucket('stats', 'children', type='stat') \
  .bucket('date_range', 'filter', Q('range', date={'lte': 'now'})) \
  .bucket(
      'dates', 'date_histogram', field='date', interval=interval,
      min_doc_count=1
  ).metric('count', 'cardinality', field='system_id', precision_threshold=40000)

  return s[:0].execute()


def search_weighted(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query('term', country=country)

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
      'models', 'terms', field='model'
  ).bucket('stats', 'children', type='stat') \
  .bucket('date_range', 'filter', Q('range', date={'lt': 'now/d'})) \
  .bucket(
      'dates', 'date_histogram', field='date', interval=interval,
      min_doc_count=1
  )

  return s[:0].execute()


def df_open_now(country, f=None, interval='month'):
  response = search_open_now(country, f=f)

  models = [x.key for x in response.aggs.models.buckets]

  obj = {x: { 0 } for x in models}

  for model in response.aggs.models.buckets:
    obj[model.key] = { model.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['now']
  )
  df.loc['total'] = df.sum()

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.sort_index()

  return df


def df_start(country, f=None, interval='month'):
  open_now = df_open_now(country, f=f, interval=interval)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'start')
  return df


def df_end(country, f=None, interval='month'):
  open_now = df_open_now(country, f=f, interval=interval)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'end')
  return df


def df_average(country, f=None, interval='month'):
  start = df_start(country, f=f, interval=interval)
  end = df_end(country, f=f, interval=interval)
  df = (start + end) / 2
  return df


def df_distinct(country, f=None, interval='month'):
  response = search_distinct(country, f=f, interval=interval)

  obj = {}

  for model in response.aggs.models.buckets:
    obj[model.key] = {}
    for date in model.stats.date_range.dates.buckets:
      obj[model.key][date.key_as_string] = date.count.value

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return df

  df = df.T.fillna(0).astype('int64')
  df.index = df.index.astype('datetime64')
  df.index.name = 'date'
  df['total'] = df.sum(axis=1)

  return df


def df_weighted(country, f=None, interval='month'):
  response = search_weighted(country, f=f, interval=interval)

  obj = {}

  for model in response.aggs.models.buckets:
    obj[model.key] = {}
    for date in model.stats.date_range.dates.buckets:
      obj[model.key][date.key_as_string] = date.doc_count

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return df

  df = df.T.fillna(0).astype('int64')
  df.index = df.index.astype('datetime64')
  df.index.name = 'date'
  df['total'] = df.sum(axis=1)

  bucket_len = [x.days for x in diff(df.index.tolist())]
  bucket_len.append((Timestamp.now()-df.index[-1]).days)

  df = df.div(bucket_len, axis='index')


  return df


def df(country, method='end', f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'distinct': df_distinct,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, f=f, interval=interval)