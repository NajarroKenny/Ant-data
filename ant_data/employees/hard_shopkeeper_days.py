from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame

from ant_data import elastic


def df(client_ids, start, end):
  """Count of shopkeeper active days, ignoring sync status."""

  s = Search(using=elastic, index='people') \
    .query('ids', type='_doc', values=client_ids)

  #TODO: use partitions to get all the results
  # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_filtering_values_with_partitions
  s.aggs.bucket('clients', 'terms', field='person_id', size=10000) \
    .bucket('stats', 'children', type='stat') \
    .bucket('date_range', 'filter', filter=Q('range', date={ 'gte': start, 'lt': end })) \
    .bucket('active', 'terms', field='active_shopkeeper')

  response = s[:0].execute()

  obj = {}
  for client in response.aggs.clients.buckets:
    obj[client.key] = {}
    for active in client.stats.date_range.active.buckets:
      obj[client.key][active.key] = active.doc_count

  df = DataFrame.from_dict(obj).T

  if df.empty:
    return df

  df.index.name = 'client_id'
  df = df.rename(columns={ 0: 'inactive', 1: 'active' })
  df['total'] = df.sum(axis=1)
  df = df.fillna(0).astype('int64')

  return df