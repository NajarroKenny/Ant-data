from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query('bool', filter=[Q('term', country=country), 
                           Q('term', doctype='kingo')])

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='closed', interval=interval) \
    .bucket('models', 'terms', field='model')

  return s[:0].execute()

def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

  obj = {}

  for date in response.aggregations.dates.buckets: 
    for model in date.models.buckets:
      obj[(date.key_as_string, model.key)] = { 'closed': model.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['closed']
  )
  
  if df.empty:
    return df

  df = df.rename_axis(['date', 'model'])
  idx = MultiIndex(
    levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
    labels=df.index.labels, names=['date', 'model']
  )
  df = DataFrame(df.values, index=idx, columns=['closed']).sort_index().reset_index()
  df = df.set_index('date')

  return df

