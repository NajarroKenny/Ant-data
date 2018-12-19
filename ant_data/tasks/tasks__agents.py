"""
Task Agents
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on tasks by agents in a given time range.

- Create date:  2018-12-15
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def search(country, start, end, f=None):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  s = Search(using=elastic, index='tasks') \
    .query('term', country=country) \
    .query('term', doctype='task') \
    .query('range', due={'gte': start, 'lt': end})

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', min_doc_count=1, size=10000)

  return s[:0].execute()


def df(country, start, end, f=None):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  response = search(country, start, end, f=f)

  obj = {}
  for agent in response.aggs.agents.buckets:
    obj[agent.key] = agent.doc_count
      
  df = DataFrame.from_dict(obj, orient='index', columns=['tasks'])

  if df.empty:
    return df

  df.index.name = 'agent_id'
  df = df.fillna(0).astype('int64').sort_index()

  return df
