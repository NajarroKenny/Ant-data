"""
Task Agents
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on tasks by agents in a given time range.

- Create date:  2018-12-15
- Update date:  2018-12-31
- Version:      1.2

Notes:
============================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(start, end, f=None):
  s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
    .query('term', country=country) \
    .query('term', doctype='task') \
    .query('range', due={'gte': start, 'lt': end})

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', min_doc_count=1, size=10000)

  return s[:0].execute()


def df(start, end, f=None):
  response = search(start, end, f=f)

  obj = {}
  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count
      
  df = DataFrame.from_dict(obj, orient='index', columns=['tasks'])

  if df.empty:
    return df

  df.index.name = 'agent_id'
  df = df.fillna(0).astype('int64').sort_index()

  return df
