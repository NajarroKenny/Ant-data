from ant_data import elastic
from elasticsearch_dsl import Search, Q
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
    .query('bool', filter=[Q('term', country=country), 
                           Q('term', doctype='client')])

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat') \
    .bucket('dates', 'date_histogram', field='date', interval=interval)
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
  return s[:0].execute()


def df_start(country, f=None, interval='month'):
  off_switcher = {
    'week': offsets.Week(day=1),
    'month': offsets.MonthBegin,
    'quarter': offsets.QuarterBegin,
    'year': offsets.YearBegin
  }
  
  curr_date = Timestamp.now()
  
  n_switcher = {
    'week': 0 if curr_date.isoweekday()==1 else 0,
    'month': 0 if curr_date.is_month_start else 0,
    'quarter': 0 if curr_date.is_quarter_start else 0,
    'year': 0 if curr_date.is_year_start else 0
  }

  curr_bucket = curr_date - off_switcher.get(interval)(n_switcher.get(interval)) 

  curr_bucket = f'{Timestamp.now().year}-{Timestamp.now().month}-01'
  curr_bucket = '2018-11-01' # AUX FOR NOW COMMENT LATER!
  open = open_now(country, f=f)
  opened = clients_opened.df(country, f=f, interval=interval)
  closed = clients_closed.df(country, f=f, interval=interval)
  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df_start = DataFrame(index=merged.index, columns=['open'])
  
  if merged.empty or str(df_start.index[-1].date()) != curr_bucket:
    return DataFrame(columns=['open'])

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
  curr_bucket = f'{Timestamp.now().year}-{Timestamp.now().month}-01'
  curr_bucket = '2018-11-01' # AUX FOR NOW COMMENT LATER!
  open = open_now(country, f=f)
  opened = clients_opened.df(country, f=f, interval=interval)
  closed = clients_closed.df(country, f=f, interval=interval)
  merged = opened.merge(closed, on='date', how='outer').sort_index()
  merged = merged.fillna(0).astype('int64')

  df_end = DataFrame(index=merged.index, columns=['open'])
  
  if merged.empty or str(df_end.index[-1].date()) != curr_bucket:
    return DataFrame(columns=['open'])

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
    axis=1).mean(axis=1)).astype('int64')
  
def df_distinct(country, f=None, interval='month'):

  response = search_distinct(country, f=f, interval=interval)

  obj = {}

  for date in response.aggs.stats.dates.buckets: 
    obj[date.key_as_string] = { date.count.value }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['open']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df

def df_weighted(country, f=None, interval='month'):
  response = search_weighted(country, f=f, interval=interval)

def df(method, country, f=None, interval='month'):
  switcher = {
    'start': df_start,
    'end':  df_end,
    'average': df_average,
    'distinct': df_distinct,
    'weighted': df_weighted
  }
  
  return switcher.get(method)(country, f=f, interval=interval)

  
