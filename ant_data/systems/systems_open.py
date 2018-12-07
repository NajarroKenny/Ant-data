"""
Systems Open
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on systems open. The 'open' count is done
in five different ways: 'start', 'end', 'average', 'distinct', and 'weighted'

- Create date:  2018-12-04
- Update date:  2018-12-06
- Version:      1.1

Notes:        
==========================
- v1.0: Initial version
- v1.1: Updated with standards from v1.0 of systems_open__model
"""
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, offsets, Series, Timestamp

from ant_data import elastic
from ant_data.systems import systems_closed, systems_opened


def open_now(country, f=None):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo'), 
        Q('term', open=True)
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  return s[:0].execute().hits.total


def search_distinct(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo')
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat') \
    .bucket('date_range', 'filter', Q('range', date={'lte': 'now'})) \
    .bucket(
      'dates', 'date_histogram', field='date', interval=interval, 
      min_doc_count=0
    ).metric(
      'count', 'cardinality', field='system_id', precision_threshold=40000
    )

  return s[:0].execute()


def search_weighted(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'has_parent', parent_type='system', query=Q('bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo'),
        ~Q('term', model='Kingo Shopkeeper')
      ])
    ).query('bool', filter=Q('term', doctype='stat')) \
    .query('range', date={'lte':'now'})

  if f is not None:
    s = s.query('bool', filter=f)
  
  s.aggs.bucket(
    'dates', 'date_histogram', field='date', interval=interval, min_doc_count=0
  )
  return s[:0].execute()
  

def df_start(country, f=None, interval='month'):
  open = open_now(country, f=f)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  
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


def df_end(country, f=None, interval='month'):
  open = open_now(country, f=f)
  opened = systems_opened.df(country, f=f, interval=interval)
  closed = systems_closed.df(country, f=f, interval=interval)
  
  if opened.empty or closed.empty:
    return DataFrame(columns=['start'])

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


def df_average(country, f=None, interval='month'):
  return DataFrame(concat(
    (df_start(country, f=f, interval=interval), df_end(country, f=f, interval=interval)), 
    axis=1).mean(axis=1)).astype('int64').rename(columns={0: 'average'})
  

def df_distinct(country, f=None, interval='month'):
  response = search_distinct(country, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.stats.date_range.dates.buckets]
  obj = { x: { 0 } for x in dates}

  for date in response.aggs.stats.date_range.dates.buckets: 
    obj[date.key_as_string] = { date.count.value }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['distinct']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df


def df_weighted(country, f=None, interval='month'):
  response = search_weighted(country, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.dates.buckets]
  obj = { x: { 0 } for x in dates}

  for date in response.aggs.dates.buckets: 
    obj[date.key_as_string] = { date.doc_count }
  
  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['weighted']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  bucket_len = [x.days for x in diff(df.index.tolist())]
  bucket_len.append((Timestamp.now()-df.index[-1]).days + 1)
  df = df.div(bucket_len, axis='index')

  return df.astype('int64')


def df(country, method=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'distinct': df_distinct,
     'weighted': df_weighted
   }
  
  if method is not None:
    return switcher.get(method)(country, f=f, interval=interval)

  else:
    start = df_start(country, f=f, interval=interval)
    end = df_end(country, f=f, interval=interval)
    average = df_average(country, f=f, interval=interval)
    distinct = df_distinct(country, f=f, interval=interval)
    weighted = df_weighted(country, f=f, interval=interval)

    return start.merge(end, on='date', how='inner')\
      .merge(average, on='date', how='inner') \
      .merge(distinct, on='date', how='inner') \
      .merge(weighted, on='date', how='inner')
  
