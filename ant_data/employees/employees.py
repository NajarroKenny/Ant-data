"""
Employees
==========================
Wrapper for all employee data-fetching functions

- Create date:  2018-12-17
- Update date:  2018-12-17
- Version:      1.0
 
Notes
==========================
- v1.0: Initial version
"""
from ant_data.employees import at_tasks as _at_tasks
from ant_data.employees import at_variable_pay as _at_variable_pay
from ant_data.employees import hard_client_days as _hard_client_days
from ant_data.employees import hierarchy as _hierarchy
from ant_data.employees import roster as _roster
from ant_data.employees import sync_status as _sync_status
from elasticsearch_dsl import Q

def at_tasks(country, agent_id, start, end, f=None):
  return _at_tasks.data(country, agent_id, start, end, f)

def at_variable_pay(agent, start, end):
  return _at_variable_pay.df(agent, start, end)

def hard_client_days(agent, start, end):
  return _hard_client_days.df(agent, start, end)

def communities(agent):
  return _hierarchy.communities(agent)

def clients(agent, agent_type, date=None):
  return _hierarchy.clients(agent, agent_type, date)

def codes(agent, start, end):
  return _hierarchy.codes(agent, start, end)

def agents():
  return _roster.agents()

def coordinators():
  return _roster.coordinators()

def person(id):
  return _roster.person(id)

def supervisors():
  return _roster.supervisors()

def agent_sync_status(country, agent_id, date=None, threshold=0):
  return _sync_status.agent_sync_status(country, agent_id, date, threshold)

def coordinator_agent_sync_status(country, coordinator_id, date=None, threshold=0):
  return _sync_status.coordinator_agent_sync_status(country, coordinator_id, date, threshold)

def sk_sync_status(country, person_id, threshold=0, date=None):
  return _sync_status.sk_sync_status(country, person_id, threshold, date)

def coordinator_sk_sync_status(country, coordinator_id, date=None, threshold=0):
  return _sync_status.coordinator_sk_sync_status(country, coordinator_id, date, threshold)
