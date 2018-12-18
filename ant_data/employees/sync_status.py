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
from elasticsearch_dsl import Q
from pandas import DataFrame

from ant_data.employees import hierarchy
from ant_data.people.people import sync_log
from ant_data.shared.helpers import local_date_str, shift_date_str
from ant_data.static.AGENT_MAPPING import AGENT_MAPPING

def agent_sync_status(agent_id, date=None, threshold=1):
  info = hierarchy.info(agent_id)
  country = info['country']

  if date is None:
    date = local_date_str(country)

  agent_id = (
    AGENT_MAPPING.get(agent_id) if agent_id in AGENT_MAPPING else agent_id
  )
  ls = sync_log(country=country, f=Q('term', agent_id=agent_id))
  ls = ls['sync_date'].max()
  ls = '' if (isinstance(ls, float)) else ls
  sync_status = True if ls >= shift_date_str(date, days=-threshold) else False
  return [ls, sync_status]
      
def coordinator_agent_sync_status(coordinator_id, date=None, threshold=1):
  info = hierarchy.info(coordinator_id)
  country = info['country']
  
  if date is None:
    date = local_date_str(country)

  agent_list = [
    AGENT_MAPPING.get(x) if x in AGENT_MAPPING else x for x in info['agent_id']]
  
  ls = sync_log(country=country, f=Q('terms', agent_id=agent_list))
  breakpoint()
  ls = DataFrame(ls.groupby('agent_id').max()['sync_date'].fillna(''))
  ls['sync_status'] = ls['sync_date'].apply(
    lambda x: True if x >= shift_date_str(date, days=-threshold) else False
  )
  
  count = len(ls)
  synced = ls['sync_status'].sum()
  perc_synced = synced/count if count != 0 else 0

  return {
    'count': count,
    'synced': synced,
    'perc_synced': perc_synced
  } 

def sk_sync_status(sk_id, threshold=1, date=None):
  pass #TODO:

def coordinator_sk_sync_status(coordinator_id, date=None, threshold=1):
  pass #TODO: