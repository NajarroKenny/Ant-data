from elasticsearch_dsl import Q
from ant_data.systems import systems_opened as _systems_opened
from ant_data.systems import systems_closed as _systems_closed
from ant_data.systems import systems_open as _systems_open

def systems_opened(country, start=None, end=None, f=None, interval='month'):
  return _systems_opened.df(country, start=start, end=end, f=f, interval=interval)

def systems_closed(country, start=None, end=None, f=None, interval='month'):
  return _systems_closed.df(country, start=start, end=end, f=f, interval=interval)

def systems_open(country, method='end', start=None, end=None, f=None, interval='month'):
  return _systems_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def kingos_opened(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='kingo'))
  return _systems_opened.df(country, start=start, end=end, f=f, interval=interval)

def kingos_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='kingo'))
  return _systems_closed.df(country, start=start, end=end, f=f, interval=interval)

def kingos_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='kingo'))
  return _systems_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def pos_opened(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='pos'))
  return _systems_opened.df(country, start=start, end=end, f=f, interval=interval)

def pos_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='pos'))
  return _systems_closed.df(country, start=start, end=end, f=f, interval=interval)

def pos_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='pos'))
  return _systems_open.df(country, method=method, start=start, end=end, f=f, interval=interval)

def no_systems_opened(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_system'))
  return _systems_opened.df(country, start=start, end=end, f=f, interval=interval)

def no_systems_closed(country, start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_system'))
  return _systems_closed.df(country, start=start, end=end, f=f, interval=interval)

def no_systems_open(country, method='end', start=None, end=None, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', doctype='no_system'))
  return _systems_open.df(country, method=method, start=start, end=end, f=f, interval=interval)