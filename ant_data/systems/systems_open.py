"""
Systems Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on systems open by model. The 'open' count is done
in five different ways: 'start', 'end', 'average', and 'weighted'

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


def search_open_now(country, start=None, end=None, f=None):
  s = Search(using=elastic, index='systems')

  if start is None and end is None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('term', open=True)
      ]
    )
  else:
    s = s.query(
      'bool', filter=[
        Q('range', opened={ 'lt': end }),
        Q('bool', should=[
          ~Q('exists', field='closed'),
          Q('range', closed={ 'gte': end })
        ])
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('models', 'terms', field='model')

  return s[:0].execute()


def df_open_now(country, start=None, end=None, f=None, interval='month'):
  response = search_open_now(country, start=start, end=end, f=f)

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


def df_start(country, start=None, end=None, f=None, interval='month'):
  open_now = df_open_now(country, start=start, end=end, f=f, interval=interval)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'start')
  return df


def df_end(country, start=None, end=None, f=None, interval='month'):
  open_now = df_open_now(country, start=start, end=end, f=f, interval=interval)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'end')
  return df


def df_average(country, start=None, end=None, f=None, interval='month'):
  start = df_start(country, start=start, end=end, f=f, interval=interval)
  end = df_end(country, start=start, end=end, f=f, interval=interval)
  df = (start + end) / 2
  return df


def df_weighted(country, start=None, end=None, f=None, interval='month'):
  print('Systems do not support method="weighted".')


def df(country, method='end', start=None, end=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, start=start, end=end, f=f, interval=interval)