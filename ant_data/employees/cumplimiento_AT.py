# TODO: Create tasks version script that generates aggregate on range
"""
Cumplimiento AT
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task status.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
import datetime as dt

from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame


from ant_data import elastic
from ant_data.employees.sync_status import agent_sync_status
from ant_data.static.AGENT_MAPPING import AGENT_MAPPING
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TIME import TZ
from ant_data.tasks.tasks import *


VISITED_F = [Q('has_child', type='history', query=Q())]
NOT_VISITED_F = [Q('bool', must_not=Q('has_child', type='history', query=Q()))]
PLANNED_F = [Q('term', planned=True)]
ADDITIONAL_F = [Q('term', planned=False)]

def data(country, agent_id, start, end, f=None):
  
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  if f is None:
    f = []

  f += [
    Q('term', agent_id=agent_id), Q('range', due={'gte': start, 'lt': end})
  ]

  df_tasks = tasks__types(country, f=f, interval='year').sum(axis=0)
  df_tasks = DataFrame(df_tasks, columns=['tasks'])
  df_tasks.index.name = 'types'

  if df_tasks.empty:
    return df_tasks

  df_visited = tasks__types(country, f=f+VISITED_F, interval='year')
  df_visited = DataFrame(df_visited.sum(axis=0), columns=['visited'])
  df_visited.index.name = 'types'
  df_effective = tasks_effective__types(country, f=f+VISITED_F, interval='year')
  df_effective = DataFrame(df_effective.sum(axis=0), columns=['effective'])
  df_effective.index.name = 'types'
  df_additional = tasks__types(country, f=f+ADDITIONAL_F, interval='year')
  df_additional = DataFrame(df_additional.sum(axis=0), columns=['additional'])
  df_additional.index.name = 'types'

  df = df_tasks.merge(df_visited, on='types', how='left')
  df = df.merge(df_effective, on='types', how='left')
  df = df.merge(df_additional, on='types', how='left')
  df = df.fillna(0).astype('int64')
  df['visited_perc'] = df['visited'].div(df['tasks'])
  df['effective_perc'] = df['effective'].div(df['visited'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  df = df.replace(np.nan, 0)

  return df
