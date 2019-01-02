
"""
Hard Paid Days
==========================
TBD

- Create date:  2018-12-20
- Update date:  2018-12-28
- Version:      1.1

Notes:
==========================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
- v1.2: Implemented composite aggregation
"""
import configparser

from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')
PAG_SIZE = 1000


def search(client_ids, start, end, after_key=None, obj={}):
  """Search active days ignoring sync status. Use the composite aggregation and
  an iterative approach to paginate through the results

  Args:
    client_ids (list): List of strings containing the client IDs to count days
    start (str): ISO8601 start date
    end (str): ISO8601 end date
    after_key (str, optional): Client ID of the last result of the previous 
      search. Used for pagination. Defaults to None.
    obj (dict, optional): Dictionary with results. Defaults to an empty dict.
  
  Returns:
    dict: obj with aggregated results
  """
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('ids', type='_doc', values=client_ids)
  
  if after_key is None:
    s.aggs.bucket(
      'comp', 'composite', size=PAG_SIZE, 
      sources=[{'clients': A('terms', field='person_id', order='asc')}]
    )
  else:
    s.aggs.bucket(
      'comp', 'composite', size=PAG_SIZE, after={ 'clients': after_key },
      sources=[{'clients': A('terms', field='person_id', order='asc')}]
    )
  
  s.aggs['comp'].bucket('stats', 'children', type='stat') \
    .bucket('date_range', 'filter', filter=Q('range', date={ 'gte': start, 'lt': end })) \
    .bucket('active', 'terms', field='active_paid')
  
  response = s[:0].execute()
  
  if 'after_key' in dir(response.aggs.comp):
    after_key = response.aggs.comp.after_key.clients
    for client in response.aggs.comp.buckets:
      obj[client.key.clients] = {}
      for active in client.stats.date_range.active.buckets:
        obj[client.key.clients][active.key] = active.doc_count
  
    return search(client_ids, start, end, after_key, obj)
  
  else:
    return obj


def df(client_ids, start, end):
  """Count of paid active days, ignoring sync status.
  
  Args:
    client_ids (list): List of strings containing the client IDs to count days
    start (str): ISO8601 start date
    end (str): ISO8601 end date
  
  Returns:
    DataFrame: Pandas DataFrame with index = 'client_id' and columns = [
      'inactive', 'active']
  """
  obj = search(client_ids, start, end)
  df = DataFrame.from_dict(obj).T

  if df.empty:
    return df

  df.index.name = 'client_id'
  df = df.rename(columns={ 0: 'inactive', 1: 'active' })
  df['total'] = df.sum(axis=1)
  df = df.fillna(0).astype('int64')

  return df