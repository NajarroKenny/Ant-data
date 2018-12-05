from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='installs') \
    .query('bool', filter=[Q('term', country=country), 
                           Q('term', doctype='install')])

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='opened', interval=interval)

  return s[:0].execute()

def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

  obj = {}

  for date in response.aggregations.dates.buckets: 
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['opened']
  )
  
  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df

