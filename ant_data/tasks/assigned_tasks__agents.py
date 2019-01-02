"""
Task Types
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on assigned tasks by agent.

- Create date:  2018-12-31
- Update date:  2018-12-31
- Version:      1.0

Notes:
============================
- v1.0: Initial version based on assigned_tasks
"""
import configparser
from copy import deepcopy

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(start=None, end=None, f=None):
  """Searches planned task documents in a date range and classifies them by 
  agent_id

  Args:
    start (str, optional): Start date in ISO8601 format. Defaults to None.
    end (str, optional): End date in ISO8601 format. Defaults to None.
    f(list, optional): List of additional query filters to passed. Filters
      must be elasticsearch_dsl Q objects.

  Returns:
    elasticsearch_dsl aggregation results buckets
  """
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('term', planned=True)

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1)

  return s[:0].execute()


def df(start=None, end=None, f=None, workflow=None, all=False):
  """Assigned tasks by agent
    
  Args:
    start (str, optional): Start date in ISO8601 format. Defaults to None.
    end (str, optional): End date in ISO8601 format. Defaults to None.
    f(list, optional): List of additional query filters to passed. Filters
      must be elasticsearch_dsl Q objects.

  Returns:
    DataFrame: Pandas DataFrame with index = agent_id and columns = ['asignadas']
  """
  g = [] if f is None else deepcopy(f)

  if workflow is not None:
    g.append(Q('has_child', type='history', query=Q('terms', workflow=workflow)))

  response = search(start=start, end=end, f=g)

  obj = {}

  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count

  df = DataFrame.from_dict(obj, orient='index', columns=['asignadas']).sort_index()
  df.index.name = 'agent_id'
  df.loc['total'] = df.sum()
  df = df.astype('int64')
  
  return df
