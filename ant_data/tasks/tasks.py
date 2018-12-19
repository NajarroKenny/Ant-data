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
from ant_data.tasks import tasks__agents as _tasks__agents
from ant_data.tasks import tasks__agents_types as _tasks__agents_types
from ant_data.tasks import tasks_effective as _tasks_effective
from ant_data.tasks import tasks_effective__agents as _tasks_effective__agents
from ant_data.tasks import tasks_effective__agents_types as _tasks_effective__agents_types
from ant_data.tasks import tasks_effective__types as _tasks_effective__types
from ant_data.tasks import tasks__planned as _tasks__planned
from ant_data.tasks import tasks__status as _tasks__status
from ant_data.tasks import tasks__types as _tasks__types
from ant_data.tasks import tasks__visited as _tasks__visited
from ant_data.tasks import tasks_workflow_list as _tasks_workflow_list
from ant_data.tasks import tasks_workflow_list__agents as _tasks_workflow_list__agents
from ant_data.tasks import tasks_workflow_list__agents_types as _tasks_workflow_list__agents_types
from ant_data.tasks import tasks_workflow_list__types as _tasks_workflow_list__types

def tasks__actions(country, start=None, end=None, f=None, interval='month'):
  return _tasks__actions.df(country, start=start, end=end, f=f, interval=interval)

def tasks__agents(country, start, end, f=None):
  return _tasks__agents.df(country, start, end, f=f)

def tasks__agents_types(country, start, end, f=None):
  return _tasks__agents_types.df(country, start, end, f=f)

def tasks_effective(country, start=None, end=None, f=None, interval='month'):
  return _tasks_effective.df(country, start=start, end=end, f=f, interval=interval)

def tasks_effective__agents(country, start=None, end=None, f=None):
  return _tasks_effective__agents.df(country, start=start, end=end, f=f)

def tasks_effective__agents_types(country, start, end, f=None):
  return _tasks_effective__agents_types.df(country, start, end, f=f)

def tasks_effective__types(country, start=None, end=None, f=None, interval='month'):
  return _tasks_effective__types.df(country, start=start, end=end, f=f, interval=interval)

def tasks__planned(country, start=None, end=None, f=None, interval='month'):
  return _tasks__planned.df(country, start=start, end=end, f=f, interval=interval)

def tasks__status(country, start=None, end=None, f=None, interval='month'):
  return _tasks__status.df(country, start=start, end=end, f=f, interval=interval)

def tasks__types(country, start=None, end=None, f=None, interval='month'):
  return _tasks__types.df(country, start=start, end=end, f=f, interval=interval)

def tasks__visited(country, start=None, end=None, f=None, interval='month'):
  return _tasks__visited.df(country, start=start, end=end, f=f, interval=interval)

def tasks_workflow_list(country, workflows, start=None, end=None, f=None, interval='month'):
  return _tasks_workflow_list.df(country, workflows, start=start, end=end, f=f, interval=interval)

def tasks_workflow_list__agents(country, workflows, start, end, f=None):
  return _tasks_workflow_list__agents.df(country, workflows, start, end, f=f)

def tasks_workflow_list__agents_types(country, workflows, start, end, f=None):
  return _tasks_workflow_list__agents_types.df(country, workflows, start, end, f=f)

def tasks_workflow_list__types(country, workflows, start=None, end=None, f=None, interval='month'):
  return _tasks_workflow_list__types.df(country, workflows, start=start, end=end, f=f, interval=interval)
