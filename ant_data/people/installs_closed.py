"""
Systems Closed by Model
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installs closed by model

- Create date:  2018-12-07
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version based on systems_closed
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: import configparser
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month', model=None, version=None, model_version=None, system_type=None, close_type=None, by='model'):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', country=country)

  if f is not None:
    s = s.query('bool', filter=f)

  filter = []

  if start is not None and end is not None:
    filter.append(Q('range', installations__closed={ 'gte': start, 'lt': end }))
  elif start is not None:
    filter.append(Q('range', installations__closed={ 'gte': start }))
  elif end is not None:
    filter.append(Q('range', installations__closed={ 'lt': end }))
  else:
    filter.append(Q('match_all'))

  if model is not None and model != []:
    model = model if isinstance(model, list) else [ model ]
    filter.append(Q('terms', installations__model=model))
  if version is not None and version != []:
    version = version if isinstance(version, list) else [ version ]
    filter.append(Q('terms', installations__version=version))
  if model_version is not None and model_version != []:
    model_version = model_version if isinstance(model_version, list) else [ model_version ]
    filter.append(Q('terms', installations__model_version=model_version))
  if system_type is not None and system_type != []:
    system_type = system_type if isinstance(system_type, list) else [ system_type ]
    filter.append(Q('terms', installations__system_type=system_type))
  if close_type is not None and close_type != []:
    close_type = close_type if isinstance(close_type, list) else [ close_type ]
    filter.append(Q('terms', installations__close_type=close_type))

  s = s.query('nested', path='installations', query=Q(
    'bool', filter=filter
  ))

  s.aggs.bucket('installations', 'nested', path='installations') \
    .bucket('start_end', 'filter', filter=Q('bool', filter=filter)) \
    .bucket('dates', 'date_histogram', field='installations.closed', interval=interval, min_doc_count=1) \
    .bucket('by', 'terms', field=f'installations.{by}')

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month', model=None, version=None, model_version=None, system_type=None, close_type=None, by='model'):
  response = search(country, start=start, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version, system_type=system_type, close_type=close_type, by=by)

  obj = {}
  for date in response.aggs.installations.start_end.dates.buckets:
    obj[date.key_as_string] = {}
    for x in date.by.buckets:
      obj[date.key_as_string][x.key] = x.doc_count

  df = DataFrame.from_dict(obj, orient='index', dtype='int64')

  if df.empty:
    return df

  df.index.name = 'date'
  df.index = df.index.astype('datetime64')
  df = df.sort_index().fillna(0).astype('int64')
  df['total'] = df.sum(axis=1)

  if system_type is None:
    if by == 'model':
      df['kingos'] = df[[col for col in df if col not in [
        'total', 'Kingo Shopkeeper', 'no_model'
      ]]].sum(axis=1)
    elif by == 'model_version':
      df['kingos'] = df[[col for col in df if col not in [
        'total', 'Kingo Shopkeeper (Android)','no_model_version'
      ]]].sum(axis=1)
    elif by == 'version':
      df['kingos'] = df[[col for col in df if col not in [
        'total', 'Android','no_version'
      ]]].sum(axis=1)

  df = df[sorted(list(df))]

  return df

