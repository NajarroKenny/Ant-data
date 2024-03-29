"""
People
==========================
Wrapper for all people data-fetching functions

- Create date:  2018-12-08
- Update date:  2018-12-31
- Version:      1.1

Notes
==========================
- v1.0: Initial version
- v1.1: g/f pattern to avoid side effects
"""
from copy import deepcopy

from elasticsearch_dsl import Q
from ant_data.people import days as _days
from ant_data.people import days__cohort as _days__cohort
from ant_data.people import people_opened as _people_opened
from ant_data.people import people_closed as _people_closed
from ant_data.people import people_open as _people_open
from ant_data.people import sync_log as _sync_log


def people_opened(country, start=None, end=None, f=None, interval='month'):
  return _people_opened.df(country, start=start, end=end, f=f, interval=interval)

def people_closed(country, start=None, end=None, f=None, interval='month'):
  return _people_closed.df(country, start=start, end=end, f=f, interval=interval)

def people_open(country, method='end', start=None, end=None, f=None, interval='month'):
  return _people_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def clients_opened(country, f=None, start=None, end=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='client'))
  return _people_opened.df(country, start=start, end=end, f=g, interval=interval)

def clients_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='client'))
  return _people_closed.df(country, start=start, end=end, f=g, interval=interval)

def clients_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='client'))
  return _people_open.df(country, method=method, start=start, end=end, f=g, interval=interval)

def employees_opened(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='employee'))
  return _people_opened.df(country, start=start, end=end, f=g, interval=interval)

def employees_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='employee'))
  return _people_closed.df(country, start=start, end=end, f=g, interval=interval)

def employees_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='employee'))
  return _people_open.df(country, method=method, start=start, end=end, f=g, interval=interval)

def no_persons_opened(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='no_person'))
  return _people_opened.df(country, start=start, end=end, f=g, interval=interval)

def no_persons_closed(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='no_person'))
  return _people_closed.df(country, start=start, end=end, f=g, interval=interval)

def no_persons_open(country, method='end', start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='no_person'))
  return _people_open.df(country, method=method, f=g, interval=interval)

def people_days(country, start=None, end=None, f=None, interval='month'):
  return _days.df(country, start=start, end=end, f=f, interval=interval)

def people_days__cohort(country, start=None, end=None, f=None, interval='month'):
  return _days__cohort.df(country, start=start, end=end, f=f, interval=interval)

def client_days(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='client'))
  return _days.df(country, start=start, end=end, f=g, interval=interval)

def client_days__cohort(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='client'))
  return _days__cohort.df(country, start=start, end=end, f=g, interval=interval)

def employee_days(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='employee'))
  return _days.df(country, start=start, end=end, f=g, interval=interval)

def employee_days__cohort(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='employee'))
  return _days__cohort.df(country, start=start, end=end, f=g, interval=interval)

def no_persons_days(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='no_person'))
  return _days.df(country, start=start, end=end, f=g, interval=interval)

def no_persons_days__cohort(country, start=None, end=None, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)
  g.append(Q('term', doctype='no_person'))
  return _days__cohort.df(country, start=start, end=end, f=g, interval=interval)

def sync_log(agent_id=None, person_id=None, f=None):
  return _sync_log.df(agent_id, person_id, f)