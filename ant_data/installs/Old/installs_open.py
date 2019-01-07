"""
Installs Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on installs open by model. The 'open' count is done
in three different ways: 'start', 'end', 'average'

- Create date:  2018-12-07
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version based on systems_open
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, date_range, MultiIndex, offsets, Series, Timestamp
import datetime

from ant_data import elastic, ROOT_DIR
from ant_data.installs import installs_closed, installs_opened
from ant_data.shared import open_df


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search_open_now(country, end=None, f=None):
  s = Search(using=elastic, index=CONFIG['ES']['INSTALLS'])

  if end is None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('term', open=True)
      ]
    )
  else:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
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


def df_open_now(country, end=None, f=None, interval='month'):
  response = search_open_now(country, end=end, f=f)

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


def df_start(country, start=None, end=None, f=None, interval='month'):
  open_now = df_open_now(country, end=end, f=f, interval=interval)
  opened = installs_opened.df(country, start=start, end=end, f=f, interval=interval)
  closed = installs_closed.df(country, start=start, end=end, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'start')
  return df


def df_end(country, start=None, end=None, f=None, interval='month'):
  open_now = df_open_now(country, end=end, f=f, interval=interval)
  opened = installs_opened.df(country, start=start, end=end, f=f, interval=interval)
  closed = installs_closed.df(country, start=start, end=end, f=f, interval=interval)
  df = open_df.open_df(opened, closed, open_now, interval, 'end')
  return df


def df_average(country, start=None, end=None, f=None, interval='month'):
  start = df_start(country, start=start, end=end, f=f, interval=interval)
  end = df_end(country, start=start, end=end, f=f, interval=interval)
  df = (start + end) / 2
  return df


def df_weighted(country, start=None, end=None, f=None, interval='month'):
  print('Installs do not support method="weighted".')


def df(country, method='end', start=None, end=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, start=start, end=end, f=f, interval=interval)

