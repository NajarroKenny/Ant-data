"""
Effective tasks agents
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task effectiveness by agent.

- Create date:  2018-12-31
- Update date:  2018-12-31
- Version:      1.0

Notes:
============================
- v1.0: Initial version based on effective_tasks
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.TASK_TYPES import SALE_VALUES
from ant_data.tasks import assigned_tasks__agents


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


WORKFLOW_LIST=[
  'active-code', 'install', 'pickup', 'register', 'swap', 'visit-install'
]


def search_sale_values(start=None, end=None, f=None):
  """Searches all history docs with workflow==sale to calculate its sale value"""
  g = [] if f is None else f[:]

  if start is not None:
    g.append(Q('range', due={ 'gte': start }))

  if end is not None:
    g.append(Q('range', due={ 'lt': end }))

  g += [Q('term', planned=True)]

  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='history') \
    .query('term', workflow='sale') \
    .query('has_parent', parent_type='task', query=Q('bool', filter=g))

  return s.scan()

def search_sale_tasks(start=None, end=None, f=None):
  """Searches for task docs that have a history with workflow==sale"""
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', doctype='task') \
    .query('term', planned=True) \
    .query('has_child', type='history', query=Q('term', workflow='sale'))

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  return s.scan()

def df_effective_sale(start=None, end=None, f=None, all=False):
  """Determines if sales are effective

  Effective means sale is sufficient for a given task type.
  """
  df = DataFrame(columns=['efectivas'])
  
  def is_effective(row):
    if row[1].startswith('gestion 3') and row[0] >= 14:
      return True
    elif row[1].startswith('gestion') and row[0] >= 7:
      return True
    elif ~row[1].startswith('gestion') and row[0] > 0:
      return True
    else:
      return False

  hits = search_sale_values(start, end, f)

  obj = {}

  for hit in hits:
    if hit.type in SALE_VALUES:
      task_id = hit.task_id
      amount = SALE_VALUES.get(hit.type, 0)
      obj[task_id] = obj.get(task_id, 0)  + amount

  if obj == {}:
    df.loc['total'] = df.sum()
    return df.astype('int64')

  df_sv = DataFrame.from_dict(obj, orient='index')
  df_sv.index.name = 'task_id'

  hits = search_sale_tasks(start, end, f)
  obj = { hit.task_id: (hit.remarks, hit.agent_id) for hit in hits }

  if obj == {}:
    df.loc['total'] = df.sum()
    return df.astype('int64')

  df_g = DataFrame.from_dict(obj, orient='index')
  df_g.index.name = 'task_id'

  df = df_sv.merge(df_g, on='task_id', how='inner')
  df['efectivas'] = df.apply(is_effective, axis=1)
  df = df.drop(['0_x', '0_y'], axis=1).groupby(1).sum()
  df = df.astype('int64').sort_index()
  df.loc['total'] = df.sum()
  df.index.name = 'agent_id'

  return df


def df(start=None, end=None, f=None, all=False):
  # Successful non-sale workflows
  df = assigned_tasks__agents.df(workflow=WORKFLOW_LIST, start=start, end=end, f=f, all=all)
  
  df = df.rename(columns={'asignadas': 'efectivas'})

  # Successful sales
  df_sale = df_effective_sale(start, end, f, all)

  if df_sale.empty:
    return df

  for element in df.index:
    df.loc[element] += df_sale['efectivas'].get(element, 0)
  
  return df
