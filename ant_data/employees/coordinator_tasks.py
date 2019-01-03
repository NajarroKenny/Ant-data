"""
Coordinator Tasks
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on coordinator tasks.

- Create date:  2018-12-18
- Update date:  2019-01-02
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.1: g/f pattern to avoid side effects
- v1.2: Cleanup and use latest task functions
"""
from copy import deepcopy

from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame

from ant_data import elastic
from ant_data.employees import hierarchy
from ant_data.tasks import additional_tasks__agents, assigned_tasks__agents, effective_tasks__agents

VISITED_F = [Q('has_child', type='history', query=Q())]

def task_vp_structure(perc_effective): #TODO:P2 Get actual formula
  """Simple function to calculate payment amount from task effectiveness.
  
  Args:
    perc_effective (float): Effectiveness percentage
  
  Returns:
    int: Effectiveness payment amount
  """
  if perc_effective < 0.50:
    return 0
  elif perc_effective < 0.75:
    return 200
  elif perc_effective < 0.85:
    return 700
  else:
    return 1000

def assigned(start, end, f=None):
  """Creates assigned task DF by merging separate tasks DF
  
  Args:
    start (str): ISO8601 start date range. 
    end (str): ISO8601 start date range. 
    f (list, optional): List of elasticsearch_dsl Q objects additional filters.

  Returns:
    DataFrame: Pandas DataFrame with index = agent_id and columns = [
      'asignadas', 'visitadas', '% visitadas', 'efectivas', '% efectivas']
  """
  f = [] if f is None else f
  
  df_assigned = assigned_tasks__agents.df(start=start, end=end, f=f)

  df_visited = assigned_tasks__agents.df(start=start, end=end, f=f+VISITED_F)
  df_visited = df_visited.rename(columns={'asignadas': 'visitadas'})
  df_effective = effective_tasks__agents.df(start=start, end=end, f=f+VISITED_F)

  df = df_assigned.merge(df_visited, on='agent_id', how='left')
  df = df.merge(df_effective, on='agent_id', how='left')
  df = df.fillna(0).astype('int64')

  df['% visitadas'] = df['visitadas'].div(df['asignadas'])
  df['% efectivas'] = df['efectivas'].div(df['visitadas'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  df = df.replace(np.nan, 0)

  df = df[['asignadas', 'visitadas', '% visitadas', 'efectivas', '% efectivas']]

  return df

def data(start, end, coordinator_id, f=None):
  """Combines assigned tasks, additional_tasks, additional variable pay tasks, 
  and task variable pay information into a single dictionary.

  Args:
    start (str): ISO8601 date interval start.
    end (str): ISO8601 date interval end.
    agent_id (str): Agent id
    f (list, optional): List of additional filters to pass to the query. The
      list is composed of Elasticserach DSL Q boolean objects. Defaults to none
  
  Returns:
    dict: Dictionary object with keys = ['assigned', 'additional', 
      'additional_vp', 'task_vp']
  """
  info = hierarchy.employee_info(coordinator_id)
  if info is None:
    return DataFrame()

  agent_list = info['agent_id']
  if agent_list==[]:
    return DataFrame()

  g = [] if f is None else deepcopy(f)

  g += [Q('terms', agent_id = agent_list)]

  df_assigned = assigned(start=start, end=end, f=g)
  df_additional = additional_tasks__agents.df(start=start, end=end, f=g)
  df_additional_vp = additional_tasks__agents.df_vp(start=start, end=end, f=g)

  df_task_vp = df_assigned[['asignadas', 'efectivas']].merge(
    DataFrame(df_additional_vp['instalación adicional']), on='agent_id',
    how='outer')
  df_task_vp = df_task_vp.fillna(0).astype('int64')
  df_task_vp['asignadas'] = df_task_vp['asignadas'] + df_task_vp['instalación adicional']
  df_task_vp['efectivas'] = df_task_vp['efectivas'] + df_task_vp['instalación adicional']
  df_task_vp = df_task_vp.drop('instalación adicional', axis=1)
  df_task_vp['% efectivas'] = df_task_vp['efectivas']/df_task_vp['asignadas']
  df_task_vp = df_task_vp.fillna(0).replace((np.inf, -np.inf), (0,0))
  df_task_vp['pago'] = df_task_vp['% efectivas'].apply(task_vp_structure)

  return {
    'assigned': df_assigned,
    'additional': df_additional,
    'additional_vp': df_additional_vp,
    'task_vp': df_task_vp
  }
