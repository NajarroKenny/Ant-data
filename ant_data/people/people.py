"""
People
==========================
Wrapper for all people data-fetching functions

- Create date:  2018-12-08
- Update date:  2018-12-14
- Version:      1.0

Notes
==========================
- v1.0: Initial version
"""
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
  if f is None:
    f = []
  f.append(Q('term', doctype='client'))
  return _people_opened.df(country, start=start, end=end, f=f, interval=interval)

def clients_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='client'))
  return _people_closed.df(country, start=start, end=end, f=f, interval=interval)

def clients_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='client'))
  return _people_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def employees_opened(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='employee'))
  return _people_opened.df(country, start=start, end=end, f=f, interval=interval)

def employees_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='employee'))
  return _people_closed.df(country, start=start, end=end, f=f, interval=interval)

def employees_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='employee'))
  return _people_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def no_persons_opened(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_person'))
  return _people_opened.df(country, start=start, end=end, f=f, interval=interval)

def no_persons_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_person'))
  return _people_closed.df(country, start=start, end=end, f=f, interval=interval)

def no_persons_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_person'))
  return _people_open.df(country, method=method,f=f, interval=interval)

def people_days(country, start=None, end=None, f=None, interval='month'):
  return _days.df(country, start=start, end=end, f=f, interval=interval)

def people_days__cohort(country, start=None, end=None, f=None, interval='month'):
  return _days__cohort.df(country, start=start, end=end, f=f, interval=interval)

def client_days(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='client'))
  return _days.df(country, start=start, end=end, f=f, interval=interval)

def client_days__cohort(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='client'))
  return _days__cohort.df(country, start=start, end=end, f=f, interval=interval)

def employee_days(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='employee'))
  return _days.df(country, start=start, end=end, f=f, interval=interval)

def employee_days__cohort(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='employee'))
  return _days__cohort.df(country, start=start, end=end, f=f, interval=interval)

def no_persons_days(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_person'))
  return _days.df(country, start=start, end=end, f=f, interval=interval)

def no_persons_days__cohort(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_person'))
  return _days__cohort.df(country, start=start, end=end, f=f, interval=interval)

def sync_log(country, f=None): # FIXME:
  return _sync_log.df(country=country, f=f)