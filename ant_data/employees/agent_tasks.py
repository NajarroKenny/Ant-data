"""
AT Tasks
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on AT tasks.

- Create date:  2018-12-11
- Update date:  2018-12-28
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.2: Add additional tasks and additional variable pay tasks to data function
"""
from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame

from ant_data import elastic
from ant_data.tasks import additional_tasks, assigned_tasks, effective_tasks


VISITED_F = [Q('has_child', type='history', query=Q())]


def task_vp_structure(perc_effective):
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
    DataFrame: Pandas DataFrame with index = tipo de tarea and columns = [
      'asignadas', 'visitadas', '% visitadas', 'efectivas', '% efectivas']
  """
  df_assigned = assigned_tasks.df(start=start, end=end, f=f)

  df_visited = assigned_tasks.df(start=start, end=end, f=f+VISITED_F)
  df_visited = df_visited.rename(columns={'asignadas': 'visitadas'})
  df_effective = effective_tasks.df(start=start, end=end, f=f+VISITED_F)

  df = df_assigned.merge(df_visited, on='tipo de tarea', how='left')
  df = df.merge(df_effective, on='tipo de tarea', how='left')
  df = df.fillna(0).astype('int64')

  df['% visitadas'] = df['visitadas'].div(df['asignadas'])
  df['% efectivas'] = df['efectivas'].div(df['visitadas'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  df = df.replace(np.nan, 0)

  df = df[['asignadas', 'visitadas', '% visitadas', 'efectivas', '% efectivas']]

  return df


def data(start, end, agent_id, f=None):
  """Combines assigned tasks, additional_tasks, additional variable pay tasks, 
  and task variable pay information
  """
  g = [] if f is None else f[:]

  g += [Q('term', agent_id=agent_id)]

  df_assigned = assigned(start=start, end=end, f=g)
  df_additional = additional_tasks.df(start=start, end=end, f=g)
  df_additional_vp = additional_tasks.df_vp(start=start, end=end, f=g)

  vp_assigned = df_assigned.at['total', 'asignadas'] \
  + df_additional_vp.at['instalación adicional', 'conteo'] if 'instalación adicional' \
  in df_additional_vp.index else df_assigned.at['total', 'asignadas']
  vp_effective = df_assigned.at['total', 'efectivas'] \
  + df_additional_vp.at['instalación adicional', 'conteo'] if 'instalación adicional' \
  in df_additional_vp.index else df_assigned.at['total', 'asignadas']

  vp_effective_perc = 0 if vp_assigned == 0 else vp_effective/vp_assigned
  vp_payment = task_vp_structure(vp_effective_perc)

  obj = {
    'asignadas': vp_assigned,
    'efectivas': vp_effective,
    '% efectivas': vp_effective_perc,
    'pago': vp_payment
  }

  df_task_vp = DataFrame(obj, index=[0])

  return {
    'assigned': df_assigned,
    'additional': df_additional,
    'additional_vp': df_additional_vp,
    'task_vp': df_task_vp
  }

