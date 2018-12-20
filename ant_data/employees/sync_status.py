"""
Coordinator Sync Status
==========================
Calculates the sync status of agents and shopkeepers for a given cs

- Create date:  2018-12-17
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from pandas import DataFrame

from ant_data.employees import hierarchy
from ant_data.people import sync_log
from ant_data.shared.helpers import local_date_str, shift_date_str
from ant_data.shopkeepers import community_shopkeepers
from ant_data.static.AGENT_MAPPING import AGENT_MAPPING


def agent_sync_status(country, agent_id, date=None, threshold=0):
  if date is None:
    date = local_date_str(country)

  agent_id = (
    AGENT_MAPPING.get(agent_id) if agent_id in AGENT_MAPPING else agent_id
  )
  ls = sync_log.df(country=country, agent_id=agent_id)
  ls = ls['sync_date'].max()
  ls = '' if (isinstance(ls, float)) else ls
  sync_threshold = shift_date_str(date, days=-threshold)
  sync_status = True if ls >= sync_threshold else False

  return [ls, sync_status, sync_threshold]


def coordinator_agent_sync_status(country, coordinator_id, date=None, threshold=0):
  info = hierarchy.info(coordinator_id)

  if date is None:
    date = local_date_str(country)

  agent_list = [
    AGENT_MAPPING.get(x) if x in AGENT_MAPPING else x for x in info['agent_id']]

  ls = sync_log.df(country=country, agent_id=agent_list)
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

def sk_sync_status(country, person_id, date=None, threshold=0):
  if date is None:
    date = local_date_str(country)

  ls = sync_log.df(country=country, person_id=person_id)
  ls = ls['sync_date'].max()
  ls = '' if (isinstance(ls, float)) else ls
  sync_threshold = shift_date_str(date, days=-threshold)
  sync_status = True if ls >= sync_threshold else False

  return [ls, sync_status, sync_threshold]


def coordinator_sk_sync_status(country, coordinator_id, date=None, threshold=0):
  info = hierarchy.info(coordinator_id)

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