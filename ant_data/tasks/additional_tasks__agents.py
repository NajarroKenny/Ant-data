#TODO: Review if the additional searches don't overlap
"""
Additional tasks agents
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on additional tasks by agent

- Create date:  2018-12-21
- Update date:  2019-01-02
- Version:      1.0

Notes:
============================
- v1.0: Initial version based on additional_tasks script
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.TASK_TYPES import ADDITIONAL_TASK_TYPES


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search_additionals(start=None, end=None, f=None):
  """Searches additional tasks and classifies them by agent and their worfklow.
  
  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    elasticsearch_dsl search: Search results hit object
  """
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('bool', should=[
        Q('term', planned=False), ~Q('exists', field='planned')
      ]) \

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1) \
    .bucket('histories', 'children', type='history') \
    .bucket('workflows', 'terms', field='workflow', size=10000, min_doc_count=1) \
    .metric('count', 'cardinality', field='task_id')

  return s[:0].execute()

def df_additionals_vp(start=None, end=None, f=None):
  """Searches generic additional tasks for variable payment calculations.
  
  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    DataFrame: DataFrame with index = agent_id and columns = ['tarea adicional']   
  """
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('bool', should=[
        Q('term', planned=False), ~Q('exists', field='planned')
      ]) \
    .exclude('has_child', type='history', query=Q('term', workflow='install'))

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1)

  response = s[:0].execute()

  obj = {}

  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count

  df = DataFrame.from_dict(obj, orient='index', columns=['tarea adicional'])

  df.index.name = 'agent_id'
  df.loc['total'] = df.sum()
  df = df.astype('int64')

  return df


def df_additional_installations(start=None, end=None, f=None):
  """Searches additional installations based on workflow type==install for
  variable payment calculations.

  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    DataFrame: Pandas DataFrame with index = 'agent_id' and columns = [
      'instalación adicional']
  """
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('bool', should=[
        Q('term', planned=False), ~Q('exists', field='planned')
      ]) \
    .query('has_child', type='history', query=Q('term', workflow='install'))

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1)

  response = s[:0].execute()

  obj = {}

  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count

  df = DataFrame.from_dict(obj, orient='index', columns=['instalación adicional'])

  df = df.sort_index()
  df.index.name = 'agent_id'
  df.loc['total'] = df.sum()
  df = df.astype('int64')

  return df


def df_additional_shopkeepers(start=None, end=None, f=None):
  """Searches additional shopkeeper tasks based on model for variable payment
  calculations.
  
  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.

  Returns:
    DataFrame: Pandas DataFrame with index = 'agent_id' and columns = [
      'venta a tendero']
  """
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('bool', should=[
        Q('term', planned=False), ~Q('exists', field='planned')
      ]) \
    .query('term', model='Kingo Shopkeeper')

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1)

  response = s[:0].execute()

  obj = {}

  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count

  df = DataFrame.from_dict(obj, orient='index', columns=['venta a tendero'])

  df = df.sort_index()
  df.index.name = 'agent_id'
  df.loc['total'] = df.sum()
  df = df.astype('int64')

  return df

def df(start=None, end=None, f=None):
  """Additional tasks classified by agent and workflow

  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
    
  Returns:
    DataFrame: Pandas DatFrame with index = agent_id and columns = task types
  """
  response = search_additionals(start=start, end=end, f=f)

  obj = {}

  for agent in response.aggs.agents.buckets:
    obj[agent.key] = {}
    for workflow in agent.histories.workflows.buckets:
      obj[agent.key][ADDITIONAL_TASK_TYPES.get(workflow.key, 'sin registro')] = workflow.count.value

  df = DataFrame.from_dict(obj, orient='index')

  df = df.sort_index()
  df.index.name = 'agent_id'
  df = df.fillna(0).astype('int64')
  df['total'] = df.sum(axis=1)

  return df


def df_vp(start=None, end=None, f=None):
  """Additional tasks for variable payment calculations.

  1. Generic additionals tasks
  2. Additonal installations
  3. Additional shopkeeper tasks

  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    DataFrame: Pandas DatFrame with index = acción and columns = [
      'tarea adicional', 'instalación adicional', 'venta a tendero']
  """
  additionals_vp = df_additionals_vp(start, end, f)
  additional_installations = df_additional_installations(
      start, end, f
  )
  additional_shopkeepers = df_additional_shopkeepers(start, end, f)

  df = additionals_vp.merge(additional_installations, on='agent_id', how='outer')
  df = df.merge(additional_shopkeepers, on='agent_id', how='outer')
  df = df.fillna(0).astype('int64').drop('total')
  df = df.sort_index()
  df.loc['total'] = df.sum()

  return df

