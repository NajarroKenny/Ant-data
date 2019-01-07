"""
Installs Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on installs open by model. The 'open' count is done
in three different ways: 'start', 'end', 'average'

- Create date:  2018-12-07
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version based on systems_open
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, date_range, MultiIndex, offsets, Series, Timestamp
import datetime

from ant_data import elastic, ROOT_DIR
from ant_data.people import installs_closed, installs_opened
from ant_data.shared import open_df


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search_open_now(country, end=None, f=None, model=None, version=None, model_version=None, system_type=None, by='model'):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', country=country)

  if f is not None:
    s = s.query('bool', filter=f)

  filter = []
  if end is None:
    filter.append(Q('term', installations__open=True))
  elif end is not None:
    filter.append(Q('bool', filter=[
        Q('range', installations__opened={ 'lt': end }),
        Q('bool', should=[
          ~Q('exists', field='installations.closed'),
          Q('range', installations__closed={ 'gte': end })
        ])
      ]
    ))

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

  s = s.query('nested', path='installations', query=Q(
    'bool', filter=filter
  ))

  s.aggs.bucket('installations', 'nested', path='installations') \
    .bucket('start_end', 'filter', filter=Q('bool', filter=filter)) \
    .bucket('by', 'terms', field=f'installations.{by}')

  return s[:0].execute()


def df_open_now(country, end=None, f=None, interval='month', model=None, version=None, model_version=None, system_type=None, by='model'):
  response = search_open_now(country, end=end, f=f, by=by, model=model, version=version, model_version=model_version, system_type=system_type)

  obj = {}

  for x in response.aggs.installations.start_end.by.buckets:
    obj[x.key] = { x.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['now']
  )
  df.loc['total'] = df.sum()

  if system_type is None:
    if by == 'model':
      df.loc['kingos'] = df.loc[[row for row in df.index if row not in [
        'total', 'Kingo Shopkeeper', 'no_model'
      ]]].sum()
    elif by == 'model_version':
      df.loc['kingos'] = df.loc[[row for row in df.index if row not in [
        'total', 'Kingo Shopkeeper (Android)', 'no_model'
      ]]].sum()
    elif by == 'version':
      df.loc['kingos'] = df.loc[[row for row in df.index if row not in [
        'total', 'Android', 'no_model'
      ]]].sum()

  if df.empty:
    return df

  df.index.name = 'by'
  df = df.sort_index()

  return df


def df(country, start=None, end=None, f=None, interval='month', model=None, version=None, model_version=None, system_type=None, by='model'):
  open_now = df_open_now(country, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version, system_type=system_type, by=by)
  opened = installs_opened.df(country, start=start, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version, system_type=system_type, by=by)
  closed = installs_closed.df(country, start=start, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version, system_type=system_type, by=by)
  df = open_df.open_df(opened, closed, open_now, interval, 'end')

  df = df[sorted(list(df))]

  return df