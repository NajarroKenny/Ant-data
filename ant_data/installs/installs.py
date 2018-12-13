from elasticsearch_dsl import Q
from ant_data.installs import installs_opened as _installs_opened
from ant_data.installs import installs_closed as _installs_closed
from ant_data.installs import installs_open as _installs_open

def installs_opened(country, f=None, interval='month'):
  return _installs_opened.df(country, f=f, interval=interval)

def installs_closed(country, f=None, interval='month'):
  return _installs_closed.df(country, f=f, interval=interval)

def installs_open(country, method='end',f=None, interval='month'):
  return _installs_open.df(country, method=method, f=f, interval=interval)

def kingos_opened(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='kingo'))
  return _installs_opened.df(country, f=f, interval=interval)

def kingos_closed(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='kingo'))
  return _installs_closed.df(country, f=f, interval=interval)

def kingos_open(country, method='end', f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='kingo'))
  return _installs_open.df(country, method=method, f=f, interval=interval)

def pos_opened(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='pos'))
  return _installs_opened.df(country, f=f, interval=interval)

def pos_closed(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='pos'))
  return _installs_closed.df(country, f=f, interval=interval)

def pos_open(country, method='end', f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='pos'))
  return _installs_open.df(country, method=method, f=f, interval=interval)

def no_systems_opened(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='no_system'))
  return _installs_opened.df(country, f=f, interval=interval)

def no_systems_closed(country, f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='no_system'))
  return _installs_closed.df(country, f=f, interval=interval)

def no_systems_open(country, method='end', f=None, interval='month'):
  if f is None:
    f = []
  f.append(Q('term', system_type='no_system'))
  return _installs_open.df(country, method=method, f=f, interval=interval)