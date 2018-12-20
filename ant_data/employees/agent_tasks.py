# TODO: Create tasks version script that generates aggregate on range
"""
AT Tasks
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on AT tasks.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame

from ant_data import elastic
from ant_data.static.AGENT_MAPPING import AGENT_MAPPING
from ant_data.static.TIME import TZ
from ant_data.tasks import additional_tasks, assigned_tasks, effective_tasks


VISITED_F = [Q('has_child', type='history', query=Q())]


def data(start, end, agent_id, f=None):

  if f is None:
    f = []

  f += [Q('term', agent_id=agent_id)]

  df_assigned = assigned_tasks.df(start=start, end=end, f=f)

  if df_assigned.empty:
    return df_assigned

  df_visited = assigned_tasks.df(start=start, end=end, f=f+VISITED_F)
  df_visited = df_visited.rename(columns={'Asignadas': 'Asignadas Visitadas'})
  df_effective = effective_tasks.df(start=start, end=end, f=f+VISITED_F)

  df_additional = additional_tasks.df(start=start, end=end, f=f)

  df = df_assigned.merge(df_visited, on='Tipo de Tarea', how='left')
  df = df.merge(df_effective, on='Tipo de Tarea', how='left')
  df = df.fillna(0).astype('int64')

  df['% visitadas'] = df['Asignadas Visitadas'].div(df['Asignadas'])
  df['% efectivas'] = df['Efectivas'].div(df['Asignadas Visitadas'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  df = df.replace(np.nan, 0)

  df = df[[
    'Asignadas', 'Asignadas Visitadas', '% visitadas',
    'Efectivas', '% efectivas'
  ]]

  return {
    'assigned': df,
    'additional': df_additional
  }

