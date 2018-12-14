"""
Tasks
==========================
Wrapper for all tasks data-fetching functions

- Create date:  2018-12-12
- Update date:  2018-12-12
- Version:      1.0

Notes
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Q
from ant_data.tasks import tasks__action_list as _tasks__action_list
from ant_data.tasks import tasks__action_list_type as _tasks__action_list_type
from ant_data.tasks import tasks__actions as _tasks__actions
from ant_data.tasks import tasks__effective as _tasks__effective
from ant_data.tasks import tasks__effective_type as _tasks__effective_type
from ant_data.tasks import tasks__planned as _tasks__planned
from ant_data.tasks import tasks__status as _tasks__status
from ant_data.tasks import tasks__types as _tasks__types
from ant_data.tasks import tasks__visited as _tasks__visited

def tasks__action_list(country, actions, f=None, interval='month'):
  return _tasks__action_list.df(country, actions=actions, f=f, interval=interval)

def tasks__action_list_type(country, actions, f=None, interval='month'):
  return _tasks__action_list_type.df(country, actions=actions, f=f, interval=interval)

def tasks__actions(country, f=None, interval='month'):
  return _tasks__actions.df(country, f=f, interval=interval)

def tasks__effective(country, f=None, interval='month'):
  return _tasks__effective.df(country, f=f, interval=interval)

def tasks__effective_type(country, f=None, interval='month'):
  return _tasks__effective_type.df(country, f=f, interval=interval)

def tasks__planned(country, f=None, interval='month'):
  return _tasks__planned.df(country, f=f, interval=interval)

def tasks__status(country, f=None, interval='month'):
  return _tasks__status.df(country, f=f, interval=interval)

def tasks__types(country, f=None, interval='month'):
  return _tasks__types.df(country, f=f, interval=interval)

def tasks__visited(country, f=None, interval='month'):
  return _tasks__visited.df(country, f=f, interval=interval)