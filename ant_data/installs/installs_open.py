"""
Installs Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on installs open by model. The 'open' count is done
in three different ways: 'start', 'end', 'average'

- Create date:  2018-12-07
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Initial version based on systems_open
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, date_range, MultiIndex, offsets, Series, Timestamp
import datetime

from ant_data import elastic
from ant_data.installs import installs_closed, installs_opened
from ant_data.shared import open_df


def search_open_now(country, f=None):
  s = Search(using=elastic, index='installs') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='install'),
        Q('term', open=True)
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('models', 'terms', field='model')

  return s[:0].execute()


def df_open_now(country, f=None, interval='month'):
  response = search_open_now(country, f=f)

  obj = {}

  for model in response.aggs.models.buckets:
    obj[model.key] = { model.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['now']
  )
  df.loc['total'] = df.sum()

  if df.empty:
    return df

  df.index.name = 'model'
  df = df.sort_index()

  return df


def df_start(country, f=None, interval='month'):
  open_now = df_open_now(country, f=f, interval=interval)
  opened = installs_opened.df(country, f=f, interval=interval)
  closed = installs_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'start')
  return df


def df_end(country, f=None, interval='month'):
  open_now = df_open_now(country, f=f, interval=interval)
  opened = installs_opened.df(country, f=f, interval=interval)
  closed = installs_closed.df(country, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'end')
  return df


def df_average(country, f=None, interval='month'):
  start = df_start(country, f=f, interval=interval)
  end = df_end(country, f=f, interval=interval)
  df = (start + end) / 2
  return df


def df_weighted(country, f=None, interval='month'):
  print('Installs do not support method="weighted".')


def df(country, method='end', f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, f=f, interval=interval)

