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
from ant_data.employees import at_variable_pay as _at_variable_pay
from ant_data.employees import hard_client_days as _hard_client_days
from ant_data.employees import hierarchy as _hierarchy
from ant_data.employees import roster as _roster
from elasticsearch_dsl import Q

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
