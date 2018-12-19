"""
CS Tasks
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on CS tasks.

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
from ant_data.static.AGENT_MAPPING import AGENT_MAPPING
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

  df_tasks = tasks__agents_types(country, start, end, f)
  df_tasks = DataFrame(df_tasks['total'])
  df_tasks = df_tasks.rename(columns={'total': 'tasks'})
  df_tasks.index.name = 'agent_id'

  if df_tasks.empty:
    return df_tasks

  df_visited = tasks__agents_types(country, start, end, f=f+VISITED_F)
  df_visited = DataFrame(df_visited['total'])
  df_visited = df_visited.rename(columns={'total': 'visited'})
  df_visited.index.name = 'agent_id'
  df_effective = tasks_effective__agents_types(country, start, end, f=f+VISITED_F)
  df_effective = DataFrame(df_effective['total'])
  df_effective = df_effective.rename(columns={'total': 'effective'})
  df_effective.index.name = 'agent_id'
  
  df = df_tasks.merge(df_visited, on='agent_id', how='left')
  df = df.merge(df_effective, on='agent_id', how='left')
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
      'effective': 'Visitas Efectivas'
    }
  )

  df = df[[
    'Tareas Asignadas', 'Asignadas Visitadas', '% visitadas', 
    'Visitas Efectivas', '% efectivas'
  ]]
  
  df = df.astype(
    {
      'Tareas Asignadas': 'int64', 
      'Asignadas Visitadas': 'int64',
      'Visitas Efectivas': 'int64'
    }
  )

  return df