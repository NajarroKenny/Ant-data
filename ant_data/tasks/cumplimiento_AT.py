"""
Cumplimiento AT
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task status.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame


from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TIME import TZ
from ant_data.tasks.tasks import *
from shared.helpers import get_local_date, shift_date


DEV_F = [Q('term', agent_id='at.cesar.tot@energysupport.gt'), Q('range', due={'gte':'2018-12-01'})] #FIXME:
VISITED_F = [Q('has_child', type='history', query=Q())]
NOT_VISITED_F = [Q('bool', must_not=Q('has_child', type='history', query=Q()))]
PLANNED_F = [Q('term', planned=True)]
ADDITIONAL_F = [Q('term', planned=False)]

def df(country, f=None, agent_id=None, start_date=None, end_date=None): #FIXME: AGENT ID
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  if f is None:
    f = []
  
  local_date = get_local_date(country, as_str=False)
  previous_day = shift_date(get_local_date(country, as_str=False), -1).isoformat()
  previous_day = '2018-12-12' #FIXME:

  if end_date is None:
    end_date = local_date.isoformat()
  if start_date is None:
    start_date = shift_date(local_date, -14).isoformat()

  base_f = [
    Q('term', agent_id=agent_id), 
    Q('range', due={'gte': start_date, 'lte': end_date})
  ]

  df_tasks = tasks__types(country, f=DEV_F, interval='day') #FIXME:

  if df_tasks.empty:
    return df_tasks

  df_visited = tasks__types(country, f=DEV_F+VISITED_F, interval='day') #FIXME:  
  df_effective = tasks_effective__type(country, f=DEV_F+VISITED_F, interval='day') #FIXME:
  df_additional = tasks__types(country, f=DEV_F+ADDITIONAL_F, interval='day') #FIXME:

  tasks = [x for x in list(df_visited)]

  if previous_day not in df_tasks.index or tasks==[]:
    return DataFrame()
  else:
    df = DataFrame(df_tasks.loc[previous_day].T)
    df.index.name = 'types'
    df = df.rename(columns={list(df.columns)[0]:'tasks'})

  if previous_day in df_visited.index:
    tmp = DataFrame(df_visited.loc[previous_day].T)
    tmp.index.name = 'types'
    tmp = tmp.rename(columns={list(tmp.columns)[0]:'visited'})
  else:
    tmp = DataFrame(index=tasks, columns=['visited'])
    tmp.index.name = 'types'

  df = df.merge(tmp, on='types', how='left')
  
  if previous_day in df_effective.index:
    tmp = DataFrame(df_effective.loc[previous_day].T)
    tmp.index.name = 'types'
    tmp = tmp.rename(columns={list(tmp.columns)[0]:'effective'})

  else:
    tmp = DataFrame(index=tasks, columns=['effective'])
    tmp.index.name = 'types'
  
  df = df.merge(tmp, on='types', how='left')

  if previous_day in df_additional.index:
    tmp = DataFrame(df_additional.loc[previous_day].T)
    tmp.index.name = 'types'
    tmp = tmp.rename(columns={list(tmp.columns)[0]:'additional'})
    
  else:
    tmp = DataFrame(index=tasks, columns=['additional'])
    tmp.index.name = 'types'
  
  df = df.merge(tmp, on='types', how='left')
  
  df = df.fillna(0).astype('int64')
  df['visited_perc'] = df['visited'].div(df['tasks'])
  df['effective_perc'] = df['effective'].div(df['visited'])
  df = df.fillna(0).replace((np.inf, -np.inf), (0,0))
  
  return df
