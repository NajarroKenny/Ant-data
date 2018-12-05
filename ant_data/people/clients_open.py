from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='people') \
    .query('bool', filter=[Q('term', country=country), 
                           Q('term', doctype='client')])

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('stats', 'children', type='stat') \
    .bucket('dates', 'date_histogram', field='date', interval=interval) \
    .metric('count', 'cardinality', field='person_id', precision_threshold=40000)
  return s[:0].execute()

def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

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

