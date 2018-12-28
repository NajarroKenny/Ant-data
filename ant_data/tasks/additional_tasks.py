#TODO: Review if the additional searches don't overlap
"""
Task Types
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task types.

- Create date:  2018-12-21
- Update date:  2018-12-28
- Version:      1.3

Notes:
============================
- v1.0: Initial version
- v1.1: Better handling of empty cases
- v1.2: Elasticsearch index names as parameters in config.ini
- v1.3: Split between VP additional tasks and cumplimiento additional tasks
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.TASK_TYPES import ADDITIONAL_TASK_TYPES


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search_additionals(start=None, end=None, f=None):
  """Searches additional tasks and classifies them by their worfklow for the 
  cumplimiento reports.
  
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

  s.aggs.bucket('histories', 'children', type='history') \
    .bucket('workflows', 'terms', field='workflow', size=10000, min_doc_count=1) \
    .metric('count', 'cardinality', field='task_id')

  return s[:0].execute()

def search_additionals_vp(start=None, end=None, f=None):
  """Searches generic additional tasks for variable payment calculations.
  
  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    int: Total number of hits in the search representing count of generic
      additional tasks.
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

  return s[:0].execute().hits.total


def search_additional_installations(start=None, end=None, f=None):
  """Searches additional installations based on workflow type==install for
  variable payment calculations.

  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
  
  Returns:
    int: Total number of hits in the search representing count of installation
      additional tasks.
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

  return s[:0].execute().hits.total


def search_additional_shopkeepers(start=None, end=None, f=None):
  """Searches additional shopkeeper tasks based on model for variable payment
  calculations.
  
  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.

  Returns:
    int: Total number of hits in the search representing count of shopkeeper
      additional tasks.
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

  return s[:0].execute().hits.total

def df(start=None, end=None, f=None):
  """Additional tasks classified by workflow for the cumplimiento report

  Args:
    start (str, optional): ISO8601 start date range. Defaults to None.
    end (str, optional): ISO8601 start date range. Defaults to None.
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.
    
  Returns:
    DataFrame: Pandas DatFrame with index = acción and columns = ['conteo']
  """
  response = search_additionals(start=start, end=end, f=f)

  obj = {}

  for workflow in response.aggs.histories.workflows.buckets:
      obj[ADDITIONAL_TASK_TYPES.get(workflow.key, 'sin registro')] = workflow.count.value

  df = DataFrame.from_dict(obj, orient='index', columns=['conteo'])

  df.index.name = 'acción'
  df.loc['total'] = df.sum()
  df = df.astype('int64')

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
      DataFrame: Pandas DatFrame with index = acción and columns = ['conteo']
    """
    additionals_vp = search_additionals_vp(start, end, f)
    additional_installations = search_additional_installations(
        start, end, f
    )
    additional_shopkeepers = search_additional_shopkeepers(start, end, f)

    obj = {
        'tarea adicional': additionals_vp,
        'instalación adicional': additional_installations,
        'venta a tendero': additional_shopkeepers
        }

    df = DataFrame.from_dict(obj, orient='index', columns=['conteo']).sort_index()

    df.loc['total'] = df.sum()
    df.index.name = 'acción'

    return df

