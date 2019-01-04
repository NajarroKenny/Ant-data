"""
People Open
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on open people. The 'open' count is done
in five different ways: 'start', 'end', 'average' and 'weighted'

- Create date:  2018-12-04
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standards from v1.1 of systems_open
- v1.2: Removed has_parent and unnecessary date filtering
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, offsets, Series, Timestamp

from ant_data import elastic, ROOT_DIR
from ant_data.people import people_closed, people_opened
from ant_data.shared.helpers import date_dt, local_date_dt

# FIXME:P1 split into shopkepeers, employees, clients, etc

CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def open_now(country, end=None, f=None):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE'])

  if end is None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('term', kingo_open=True) # FIXME: EMPLOYEES, clients, shopkeepers, etc
      ]
    )
  else:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('range', kingo_opened={ 'lt': end }), # FIXME: EMPLOYEES, clients, shopkeepers, etc
        Q('bool', should=[
          ~Q('exists', field='kingo_closed'),
          Q('range', kingo_closed={ 'gte': end }) # FIXME: EMPLOYEES, clients, shopkeepers, etc
        ])
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  return s[:0].execute().hits.total


def search_weighted(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
      .query('term', country=country)

  if f is not None:
      s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat')

  if start is not None and end is not None:
    s.aggs['stats'] \
      .bucket('date_filter', 'filter', Q('range', date={
        'gte': start, 'lt': end
      }))
  elif start is not None:
    s.aggs['stats'] \
      .bucket('date_filter', 'filter', Q('range', date={
        'gte': start, 'lt': 'now/d'
      }))
  elif end is not None:
    s.aggs['stats'] \
      .bucket('date_filter', 'filter', Q('range', date={'lt': end}))
  else:
    s.aggs['stats'] \
      .bucket('date_filter', 'filter', Q('range', date={'lt': 'now/d'}))

  s.aggs['stats']['date_filter'] \
    .bucket('kingo', 'filter', ~Q('term', model='Kingo Shopkeeper')) \
    .bucket('dates', 'date_histogram', field='date', interval=interval, min_doc_count=1)
    #FIXME: .bucket('kingo', 'filter', ~Q('term', model='Kingo Shopkeeper'))  hacky

  return s[:0].execute()


def df_start(country, start=None, end=None, f=None, interval='month'):

  if f is None:
    f = []

  open = open_now(country, end=end, f=f)
  opened = people_opened.df(country, start=start, end=end, f=f, interval=interval)
  closed = people_closed.df(country, start=start, end=end, f=f, interval=interval)

  if opened.empty or closed.empty:
    return DataFrame(columns=['start'])

  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df = DataFrame(index=merged.index, columns=['start'])

  if merged.empty:
    return DataFrame(columns=['start'])

  rev_idx = merged.index.tolist()
  rev_idx.reverse()

  for i in range(len(rev_idx)):
    if i == 0:
      df.loc[rev_idx[i]] = (
        open - merged['opened'].get(rev_idx[i], 0)
        + merged['closed'].get(rev_idx[i], 0)
      )

    elif i > 0:
      df.loc[rev_idx[i]] = (
        df.loc[rev_idx[i-1]] - merged['opened'].get(rev_idx[i], 0)
        + merged['closed'].get(rev_idx[i], 0)
      )

  return df


def df_end(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []

  open = open_now(country, end=end, f=f)
  opened = people_opened.df(country, start=start, end=end, f=f, interval=interval)
  closed = people_closed.df(country, start=start, end=end, f=f, interval=interval)

  if opened.empty or closed.empty:
    return DataFrame(columns=['end'])

  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df = DataFrame(index=merged.index, columns=['end'])

  if merged.empty:
    return DataFrame(columns=['end'])

  rev_idx = merged.index.tolist()
  rev_idx.reverse()

  for i in range(len(rev_idx)):
    if i == 0:
      df.loc[rev_idx[i]] = open

    elif i > 0:
      df.loc[rev_idx[i]] = (
        df.loc[rev_idx[i-1]] - merged['opened'].get(rev_idx[i-1], 0)
        + merged['closed'].get(rev_idx[i-1], 0)
      )

  return df


def df_average(country, start=None, end=None, f=None, interval='month'):
  return DataFrame(concat(
    (df_start(country, start=start, end=end, f=f, interval=interval), df_end(country, start=start, end=end, f=f, interval=interval)),
    axis=1).mean(axis=1)).astype('int64').rename(columns={0: 'average'})


def df_weighted(country, start=None, end=None, f=None, interval='month'):
  response = search_weighted(country, start=start, end=end, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.stats.date_filter.kingo.dates.buckets]
  obj = {x: { 0 } for x in dates}

  for date in response.aggs.stats.date_filter.kingo.dates.buckets:
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['weighted']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  bucket_len = [x.days for x in diff(df.index.tolist())]

  if end is not None:
    bucket_len.append((date_dt(end) - df.index[-1].date()).days)
  else:
    bucket_len.append((local_date_dt(country) - df.index[-1].date()).days)

  df = df.div(bucket_len, axis='index')

  return df.astype('int64')


def df(country, method='end', start=None, end=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, start=start, end=end, f=f, interval=interval)
