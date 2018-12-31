"""
Installs
==========================
Wrapper for all installs data-fetching functions

- Create date:  2018-12-08
- Update date:  2018-12-31
- Version:      1.2

Notes
==========================
- v1.0: Initial version
- v1.1: Added functions to break down installs and pickups by open and close
        types, respectively
- v1.2: g/f pattern to avoid side effects
"""
from copy import deepcopy

from elasticsearch_dsl import Q
from ant_data.installs import installs_closed as _installs_closed
from ant_data.installs import installs_closed__type as _installs_closed__type
from ant_data.installs import installs_open as _installs_open
from ant_data.installs import installs_opened as _installs_opened
from ant_data.installs import installs_opened__type as _installs_opened__type


def installs_closed(country, start=None, end=None, f=None, interval='month'):
  return _installs_closed.df(country, start=start, end=end, f=f, interval=interval)

def installs_closed__type(country, start=None, end=None, f=None, interval='month'):
  return _installs_closed__type.df(country, start=start, end=end, f=f, interval=interval)

def installs_open(country, method='end', start=None, end=None, f=None, interval='month'):
  return _installs_open.df(country, method=method,  start=start, end=end, f=f, interval=interval)

def installs_opened(country, start=None, end=None, f=None, interval='month'):
  return _installs_opened.df(country, start=start, end=end, f=f, interval=interval)

def installs_opened__type(country, start=None, end=None, f=None, interval='month'):
  return _installs_opened__type.df(country, start=start, end=end, f=f, interval=interval)

def kingos_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='kingo'))
  return _installs_closed.df(country, start=start, end=end, f=g, interval=interval)

def kingos_closed__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='kingo'))
  return _installs_closed__type.df(country, start=start, end=end, f=g, interval=interval)

def kingos_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='kingo'))
  return _installs_open.df(country, method=method, start=start, end=end, f=g, interval=interval)

def kingos_opened(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='kingo'))
  return _installs_opened.df(country, start=start, end=end, f=g, interval=interval)

def kingos_opened__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='kingo'))
  return _installs_opened__type.df(country, start=start, end=end, f=g, interval=interval)

def pos_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='pos'))
  return _installs_closed.df(country, start=start, end=end, f=g, interval=interval)

def pos_closed__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='pos'))
  return _installs_closed__type.df(country, start=start, end=end, f=g, interval=interval)

def pos_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='pos'))
  return _installs_open.df(country, method=method, start=start, end=end, f=g, interval=interval)

def pos_opened(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='pos'))
  return _installs_opened.df(country, start=start, end=end, f=g, interval=interval)

def pos_opened__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='pos'))
  return _installs_opened__type.df(country, start=start, end=end, f=g, interval=interval)

def no_systems_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='no_system'))
  return _installs_closed.df(country, start=start, end=end, f=g, interval=interval)

def no_systems_closed__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='no_system'))
  return _installs_closed__type.df(country, start=start, end=end, f=g, interval=interval)

def no_systems_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='no_system'))
  return _installs_open.df(country, method=method, start=start, end=end, f=g, interval=interval)

def no_systems_opened(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='no_system'))
  return _installs_opened.df(country, start=start, end=end, f=g, interval=interval)

def no_systems_opened__type(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', system_type='no_system'))
  return _installs_opened__type.df(country, start=start, end=end, f=g, interval=interval)