"""
Tasks
==========================
Wrapper for all tasks data-fetching functions

- Create date:  2018-12-12
- Update date:  2018-12-15
- Version:      1.2

Notes
==========================
- v1.0: Initial version
- v1.1: Added functions to break down by task types
- v1.2: Added functions to break down by agent and task types
"""
from ant_data.tasks import tasks__actions as _tasks__actions
from ant_data.tasks import tasks__agents_types as _tasks__agents_types
from ant_data.tasks import tasks_effective as _tasks_effective
from ant_data.tasks import tasks_effective__agents_types as _tasks_effective__agents_types
from ant_data.tasks import tasks_effective__types as _tasks_effective__types
from ant_data.tasks import tasks__planned as _tasks__planned
from ant_data.tasks import tasks__status as _tasks__status
from ant_data.tasks import tasks__types as _tasks__types
from ant_data.tasks import tasks__visited as _tasks__visited
from ant_data.tasks import tasks_workflow_list as _tasks_workflow_list
from ant_data.tasks import tasks_workflow_list__agents_types as _tasks_workflow_list__agents_types
from ant_data.tasks import tasks_workflow_list__types as _tasks_workflow_list__types

def tasks__actions(country, f=None, interval='month'):
  return _tasks__actions.df(country, f=f, interval=interval)

def tasks__agents_types(country, start, end, f=None):
  return _tasks__agents_types.df(country, start, end, f=f)

def tasks_effective(country, f=None, interval='month'):
  return _tasks_effective.df(country, f=f, interval=interval)

def tasks_effective__agents_types(country, start, end, f=None):
  return _tasks_effective__agents_types.df(country, start, end, f=f)

def tasks_effective__types(country, f=None, interval='month'):
  return _tasks_effective__types.df(country, f=f, interval=interval)

def tasks__planned(country, f=None, interval='month'):
  return _tasks__planned.df(country, f=f, interval=interval)

def tasks__status(country, f=None, interval='month'):
  return _tasks__status.df(country, f=f, interval=interval)

def tasks__types(country, f=None, interval='month'):
  return _tasks__types.df(country, f=f, interval=interval)

def tasks__visited(country, f=None, interval='month'):
  return _tasks__visited.df(country, f=f, interval=interval)

def tasks_workflow_list(country, workflows, f=None, interval='month'):
  return _tasks_workflow_list.df(country, workflows, f=f, interval=interval)

def tasks_workflow_list__agents_types(country, workflows, start, end, f=None):
  return _tasks_workflow_list__agents_types.df(country, workflows, start, end, f=f)

def tasks_workflow_list__types(country, workflows, f=None, interval='month'):
  return _tasks_workflow_list__types.df(country, workflows, f=f, interval=interval)
