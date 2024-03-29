"""
Credits
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits

- Create date:  2018-11-30
- Update date:  2018-12-26
- Version:      1.3

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
- v1.2: Added free, paid, iva, commission filters
- v1.3: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, paid=False, interval='month', model=None, version=None, model_version=None):

  mv = []
  if model is not None and model != []:
    model = model if isinstance(model, list) else [ model ]
    mv.append(Q('terms', model=model))
  if version is not None and version != []:
    version = version if isinstance(version, list) else [ version ]
    mv.append(Q('terms', version=version))
  if model_version is not None and model_version != []:
    model_version = model_version if isinstance(model_version, list) else [ model_version ]
    mv.append(Q('terms', model_version=model_version))

  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', country=country) \
    .query('term', doctype='stat') \
    .query('bool', must_not=[Q('term', model='Kingo Shopkeeper')]) \
    .query('bool', filter=mv)
    # .query('has_parent', parent_type='person', query=Q('term', doctype='client')) \

    #FIXME: hacky
    # FIXME: system type = 'kingo'

  if paid:
    s = s.query('term', active_paid=True)
  else:
    s = s.query('term', active=True)

  if start is not None:
    s = s.query('bool', filter=Q('range', date={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', date={ 'lt': end }))
  if f is not None:
    s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

  s.aggs.bucket(
    'dates', 'date_histogram', field='date', interval=interval,
    min_doc_count=1
  ) \
    .metric('count', 'cardinality', field='system_id', precision_threshold=40000)

  return s[:0].execute()


def df(country, start=None, end=None, f=None, paid=False, interval='month', model=None, version=None, model_version=None):
  response = search(country, start=start, end=end, paid=paid, f=f, interval=interval, model=model, version=version, model_version=model_version)

  obj = {}
  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = {
      'active_systems': date.count.value
    }

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  df = df.fillna(0).astype('int64')

  return df
