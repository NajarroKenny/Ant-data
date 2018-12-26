"""
Coordinator Sync Status
==========================
Calculates the sync status of agents and shopkeepers for a given cs

- Create date:  2018-12-26
- Update date:
- Version:      1.1

Notes:
==========================
- v1.0: Initial version
- v1.0: Replace AGENT_MAPPING.py dependency with Elasticsearch query
"""
from elasticsearch_dsl import Search
from pandas import DataFrame

from ant_data import elastic
from ant_data.employees import agent_mapping, hierarchy
from ant_data.people import sync_log
from ant_data.shared.helpers import local_date_str, shift_date_str
from ant_data.shopkeepers import community_shopkeepers


def agent_sync_status(agent_id, date=None, threshold=0):
  info = hierarchy.agent_info(agent_id)
  country = info['country']
  agent_map = agent_mapping.df()
  
  if date is None:
    date = local_date_str(country)

  agent_id = (
    agent_map.loc[agent_id].squeeze() if agent_id in agent_map.index else agent_id
  )
  
  ls = sync_log.df(agent_id=agent_id)
  ls = ls['sync_date'].max()
  ls = '' if (isinstance(ls, float)) else ls
  sync_threshold = shift_date_str(date, days=-threshold)
  sync_status = True if ls >= sync_threshold else False

  return [ls, sync_status, sync_threshold]


def coordinator_agent_sync_status(coordinator_id, date=None, threshold=0):
  info = hierarchy.agent_info(coordinator_id)
  country = info['country']
  agent_map = agent_mapping.df()

  if date is None:
    date = local_date_str(country)

  agent_list = [
    agent_map.loc[x].squeeze() if x in agent_map.index else x for x in info['agent_id']]

  ls = sync_log.df(agent_id=agent_list)
  ls = DataFrame(ls.groupby('agent_id').max()['sync_date'].fillna(''))
  ls['sync_status'] = ls['sync_date'].apply(
    lambda x: True if x >= shift_date_str(date, days=-threshold) else False
  )

  count = len(ls)
  synced = ls['sync_status'].sum()
  perc_synced = synced/count if count != 0 else 0

  obj = {
    'coordinator_id': coordinator_id,
    'count': count,
    'synced': synced,
    'perc_synced': perc_synced
  }

  df = DataFrame(obj, index=[0])

  return df

def sk_sync_status(person_id, date=None, threshold=0):
  s = Search(using=elastic, index='people') \
    .query('term', doctype='client') \
    .query('term', person_id=person_id)
  
  res = s[:10000].execute()

  country = max([hit.country for hit in res.hits])

  if date is None:
    date = local_date_str(country)

  ls = sync_log.df(person_id=person_id)
  ls = ls['sync_date'].max()
  ls = '' if (isinstance(ls, float)) else ls
  sync_threshold = shift_date_str(date, days=-threshold)
  sync_status = True if ls >= sync_threshold else False

  return [ls, sync_status, sync_threshold]


def coordinator_sk_sync_status(coordinator_id, date=None, threshold=0):
  info = hierarchy.agent_info(coordinator_id)
  country = info['country']

  if date is None:
    date = local_date_str(country)

  com_list = info['community_id']
  sk_list = community_shopkeepers.shopkeepers('Guatemala', com_list)

  ls = sync_log.df(country=country, person_id=sk_list)
  ls = DataFrame(ls.groupby('person_id').max()['sync_date'].fillna(''))
  ls['sync_status'] = ls['sync_date'].apply(
    lambda x: True if x >= shift_date_str(date, days=-threshold) else False
  )

  count = len(ls)
  synced = ls['sync_status'].sum()
  perc_synced = synced/count if count != 0 else 0

  obj = {
    'coordinator_id': coordinator_id,
    'count': count,
    'synced': synced,
    'perc_synced': perc_synced
  }

  df = DataFrame(obj, index=[0])

  return df