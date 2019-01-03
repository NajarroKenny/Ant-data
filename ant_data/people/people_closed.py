"""
People Closed
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on people closed

- Create date:  2018-12-04
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_closed
- v1.2: Removed date filtering
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', country=country)

  if start is not None:
    s = s.query('bool', filter=Q('range', kingo_closed={ 'gte': start })) # FIXME: EMPLOYEES, clients, shopkeepers, etc
  if end is not None:
    s = s.query('bool', filter=Q('range', kingo_closed={ 'lt': end })) # FIXME: EMPLOYEES, clients, shopkeepers, etc
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('dates', 'date_histogram', field='kingo_closed', interval=interval, min_doc_count=1) # FIXME: EMPLOYEES, clients, shopkeepers, etc

  return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
  response = search(country, start=start, end=end, f=f, interval=interval)

  obj = {}
  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = { date.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['closed']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  return df

