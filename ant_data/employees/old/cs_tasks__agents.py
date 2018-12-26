"""
CS Tasks Agent
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on CS tasks per agent.

- Create date:  2018-12-18
- Update date:  2018-12-18
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame

from ant_data import elastic
from ant_data.employees import hierarchy
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TASK_TYPES import TASK_TYPES
from ant_data.static.TIME import TZ
from ant_data.tasks.tasks import *


VISITED_F = [Q('has_child', type='history', query=Q())]
ADDITIONAL_F = [Q('term', planned=False)]


def data(country, coordinator_id, start, end, f=None):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  info = hierarchy.info(coordinator_id)
  if info is None:
    return DataFrame()

  agent_list = info['agent_id']

  if agent_list==[]:
    return DataFrame()

  if f is None:
    f = []

  f += [
    Q('range', due={'gte': start, 'lt': end}),
    Q('terms', agent_id = agent_list)
  ]

  df_tasks = tasks__agents(country, start, end, f)

  if df_tasks.empty:
    return df_tasks

  df_visited = tasks__agents(country, start, end, f=f+VISITED_F)
  df_visited = df_visited.rename(columns={'tasks': 'visited'})
  df_effective = tasks_effective__agents(country, start, end, f=f+VISITED_F)
  df_effective = df_effective.rename(columns={'tasks': 'effective'})
  df_additional = tasks__agents(country, start=start, end=end, f=f+ADDITIONAL_F)
  df_additional = df_additional.rename(columns={'tasks': 'additional'})

  df = df_tasks.merge(df_visited, on='agent_id', how='left')
  df = df.merge(df_effective, on='agent_id', how='left')
  df = df.merge(df_additional, on='agent_id', how='left')
  df = df.fillna(0).astype('int64')
  df['% visitadas'] = df['visited'].div(df['tasks'])
  df['% efectivas'] = df['effective'].div(df['visited'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  df = df.replace(np.nan, 0)
  df.index.name = 'Agente'
  df.loc['Total'] = df.sum(axis=0)
  df.at['Total', '% visitadas'] = df.at['Total', 'visited']/df.at['Total', 'tasks'] if df.at['Total', 'tasks'] !=0 else 0
  df.at['Total', '% efectivas'] = df.at['Total', 'effective']/df.at['Total', 'visited'] if df.at['Total', 'visited'] !=0 else 0

  df = df.rename(
    index=TASK_TYPES,
    columns= {
      'tasks': 'Tareas Asignadas',
      'visited': 'Asignadas Visitadas',
      'effective': 'Visitas Efectivas',
      'additional': 'Tareas Adicionales'
    }
  )

  df = df[[
    'Tareas Asignadas', 'Asignadas Visitadas', '% visitadas',
    'Visitas Efectivas', '% efectivas', 'Tareas Adicionales'
  ]]

  df = df.astype(
    {
      'Tareas Asignadas': 'int64',
      'Asignadas Visitadas': 'int64',
      'Visitas Efectivas': 'int64',
      'Tareas Adicionales': 'int64'
    }
  )

  return df