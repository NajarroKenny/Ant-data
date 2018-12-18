"""
People Open
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on open people. The 'open' count is done
in five different ways: 'start', 'end', 'average', 'distinct', and 'weighted'

- Create date:  2018-12-04
- Update date:  2018-12-13
- Version:      1.3

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standards from v1.1 of systems_open
- v1.2: Removed has_parent and unnecessary date filtering
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
"""
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, offsets, Series, Timestamp

from ant_data import elastic
from ant_data.people import people_closed, people_opened


def open_now(country, start=None, end=None, f=None):
  s = Search(using=elastic, index='people')

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

  return s[:0].execute().hits.total


def search_distinct(country, start=None, end=None, f=None, interval='month'):
    s = Search(using=elastic, index='people') \
        .query('term', country=country)

    if start is not None:
      s = s.query('bool', filter=Q('range', date={ 'gte': start }))
    if end is not None:
      s = s.query('bool', filter=Q('range', date={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('stats', 'children', type='stat') \
        .bucket('dates', 'date_histogram', field='date', interval=interval, min_doc_count=1) \
        .metric('count', 'cardinality', field='person_id')

    return s[:0].execute()


def search_weighted(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
      .query('term', country=country)

  if start is not None:
    s = s.query('bool', filter=Q('range', date={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', date={ 'lt': end }))
  if f is not None:
      s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat') \
      .bucket('date_filter', 'filter', Q('range', date={'lt': 'now/d'})) \
      .bucket('dates', 'date_histogram', field='date', interval=interval, min_doc_count=1)

  return s[:0].execute()


def df_start(country, start=None, end=None, f=None, interval='month'):

  if f is None:
    f = []

  open = open_now(country, f=f)
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

  open = open_now(country, f=f)
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


def df_distinct(country, start=None, end=None, f=None, interval='month'):
  response = search_distinct(country, start=start, end=end, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.stats.dates.buckets]
  obj = {x: { 0 } for x in dates}

  for date in response.aggs.stats.dates.buckets:
    obj[date.key_as_string] = { date.count.value }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['distinct']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df


def df_weighted(country, start=None, end=None, f=None, interval='month'):
  response = search_weighted(country, start=start, end=end, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.stats.date_filter.dates.buckets]
  obj = {x: { 0 } for x in dates}

  for date in response.aggs.stats.date_filter.dates.buckets:
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['weighted']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  bucket_len = [x.days for x in diff(df.index.tolist())]
  bucket_len.append((Timestamp.now()-df.index[-1]).days)

  df = df.div(bucket_len, axis='index')

  return df.astype('int64')


def df(country, method='end', start=None, end=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'distinct': df_distinct,
     'weighted': df_weighted
   }

  return switcher.get(method)(country, start=start, end=end, f=f, interval=interval)
