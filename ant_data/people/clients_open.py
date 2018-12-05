from ant_data import elastic
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, offsets, Series, Timestamp
from ant_data.people import clients_closed, clients_opened

def open_now(country, f=None):
  s = Search(using=elastic, index='people') \
    .query('bool', filter=[Q('term', country=country), 
    Q('term', doctype='client'), Q('term', open=True)])

  if f is not None:
    s = s.query('bool', filter=f)

  return s[:0].execute().hits.total

def search_weighted(country, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
        .query('has_parent', parent_type='person', query=Q('bool', 
          filter=[Q('term', country=country), Q('term', doctype='client')])) \
        .query('bool', filter=Q('term', doctype='stat')) \
        .query('range', date={'lte':'now'})

  if f is not None:
    s = s.query('bool', filter=f)
  
  s.aggs.bucket('dates', 'date_histogram', field='date', interval=interval)
  return s[:0].execute()


def search_distinct(country, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
    .query('bool', filter=[Q('term', country=country), 
                           Q('term', doctype='client')])

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat') \
    .bucket('dates', 'date_histogram', field='date', interval=interval) \
    .metric('count', 'cardinality', field='person_id', precision_threshold=40000)
  
  s.post_filter('range', date={'lte': 'now'})
  
  return s[:0].execute()


def df_start(country, f=None, interval='month'):
  open = open_now(country, f=f)
  opened = clients_opened.df(country, f=f, interval=interval)
  closed = clients_closed.df(country, f=f, interval=interval)
  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df_start = DataFrame(index=merged.index, columns=['start'])
  
  if merged.empty:
    return DataFrame(columns=['start'])

  rev_idx = merged.index.tolist()
  rev_idx.reverse()

  for i in range(len(rev_idx)):
    if i == 0:
      df_start.loc[rev_idx[i]] = (
        open - merged['opened'].get(rev_idx[i], 0)
        + closed['closed'].get(rev_idx[i], 0)
      )
  
    elif i > 0:
      df_start.loc[rev_idx[i]] = (
        df_start.loc[rev_idx[i-1]] - merged['opened'].get(rev_idx[i], 0)
        + closed['closed'].get(rev_idx[i], 0)
      )

  return df_start

def df_end(country, f=None, interval='month'):
  open = open_now(country, f=f)
  opened = clients_opened.df(country, f=f, interval=interval)
  closed = clients_closed.df(country, f=f, interval=interval)
  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df_end = DataFrame(index=merged.index, columns=['end'])
  
  if merged.empty:
    return DataFrame(columns=['end'])

  rev_idx = merged.index.tolist()
  rev_idx.reverse()

  for i in range(len(rev_idx)):
    if i == 0:
      df_end.loc[rev_idx[i]] = open

    elif i > 0:
      df_end.loc[rev_idx[i]] = (
        df_end.loc[rev_idx[i-1]] - merged['opened'].get(rev_idx[i-1], 0)
        + closed['closed'].get(rev_idx[i-1], 0)
      )

  return df_end

def df_average(country, f=None, interval='month'):
  return DataFrame(concat(
    (df_start(country, f=f, interval=interval), df_end(country, f=f, interval=interval)), 
    axis=1).mean(axis=1)).astype('int64').rename(columns={0: 'average'})
  
def df_distinct(country, f=None, interval='month'):

  response = search_distinct(country, f=f, interval=interval)

  obj = {}

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

def df_weighted(country, f=None, interval='month'):
  response = search_weighted(country, f=f, interval=interval)

  obj = {}

  for date in response.aggs.dates.buckets: 
    obj[date.key_as_string] = { date.doc_count }
  
  df_weighted = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['weighted']
  )

  if df_weighted.empty:
    return df_weighted

  df_weighted.index.name = 'date'
  df_weighted = df_weighted.reindex(df_weighted.index.astype('datetime64')).sort_index()
  bucket_len = [x.days for x in diff(df_weighted.index.tolist())]
  bucket_len.append((Timestamp.now()-df_weighted.index[-1]).days)

  df_weighted = df_weighted.div(bucket_len, axis='index')

  return df_weighted.astype('int64')

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

    return start.merge(end, on='date').merge(average, on='date') \
      .merge(distinct, on='date').merge(weighted, on='date')
  
