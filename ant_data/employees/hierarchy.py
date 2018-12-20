
"""
Index hierarchy
==========================
Indexes roster and community master information in hierarchy index

- Create date:  2018-12-15
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
import datetime as dt
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import bulk
from pandas import DataFrame, Timestamp

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def index(hierarchy_id, country, cm, agents, coordinators, supervisors):
  """Index a hierarchy"""

  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  docs = []

  for row in agents.iterrows():
    (key, agent) = row
    if agent['coordinator_ref_id'] == '':
      community_ids = cm[cm['agent_id']==key]['community_id'].tolist()
      community_ids =  list(filter(None, set(community_ids)))
    else:
      agent_ids = agents[agents['coordinator_id']==agent['coordinator_ref_id']].index.tolist()
      agent_ids = list(filter(None, set(agent_ids)))
      community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
      community_ids = list(filter(None, set(community_ids)))
    doc = {
        "_index": "hierarchy",
        "_type": "_doc",
        "_id": f'{hierarchy_id}_{key}',
        "hierarchy_id": hierarchy_id,
        "country": country,
        "doctype": agent['role_id'],
        "agent_id": key,
        "coordinator_id": agent['coordinator_id'],
        "supervisor_id": agent['supervisor_id'],
        "community_id": community_ids
      }
    docs.append(doc)

  for row in coordinators.iterrows():
    (key, coordinator) = row
    agent_ids = agents[agents['coordinator_id']==key].index.tolist()
    agent_ids = list(filter(None, set(agent_ids)))
    community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
    community_ids = list(filter(None, set(community_ids)))
    doc = {
      "_index": "hierarchy",
      "_type": "_doc",
      "_id": f'{hierarchy_id}_{key}',
      "hierarchy_id": hierarchy_id,
      "country": country,
      "doctype": coordinator['role_id'],
      "agent_id": agent_ids,
      "coordinator_id": key,
      "supervisor_id": coordinator['supervisor_id'],
      "community_id": community_ids
    }
    docs.append(doc)

  for row in supervisors.iterrows():
    (key, supervisor) = row
    agent_ids = agents[agents['supervisor_id']==key].index.tolist()
    agent_ids = list(filter(None, set(agent_ids)))
    coordinator_ids = coordinators[coordinators['supervisor_id']==key].index.tolist()
    coordinator_ids = list(filter(None, set(coordinator_ids)))
    community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
    community_ids = list(filter(None, set(community_ids)))
    doc = {
        "_index": "hierarchy",
        "_type": "_doc",
        "_id": f'{hierarchy_id}_{key}',
        "hierarchy_id": hierarchy_id,
        "country": country,
        "doctype": supervisor['role_id'],
        "agent_id": agent_ids,
        "coordinator_id": coordinator_ids,
        "supervisor_id": key,
        "community_id": community_ids
      }
    docs.append(doc)


  bulk(elastic, docs)


def hierarchy_list(country):
  """Last 100 hierarchy_ids from the hierarchy index."""

  s = Search(using=elastic, index='hierarchy') \
    .query('term', country=country)

  s.aggs \
    .bucket('hierarchy_id', 'terms', field='hierarchy_id', order={ '_key': 'desc' }, size=100)

  response = s[:0].execute()

  ids = []
  for hierarchy_id in response.aggs.hierarchy_id:
    ids.append(hierarchy_id.key)

  return ids


def latest_hierarchy(end=None):
  """The latest hierarchy (<= optional end parameter)."""

  s = Search(using=elastic, index='hierarchy')

  if end is not None:
    s = s.query('range', hierarchy_id={ 'lte': end })

  s.aggs \
    .bucket('hierarchy_id', 'terms', field='hierarchy_id', order={ '_key': 'desc' }, size=1)

  response = s[:0].execute()

  ids = []
  for hierarchy_id in response.aggs.hierarchy_id:
    ids.append(hierarchy_id.key)

  return ids[0] if len(ids) == 1 else ''


def agent_info(agent, hierarchy_id=None):
  """Agent information from the hierarchy"""

  if hierarchy_id == None:
    hierarchy_id = latest_hierarchy()

  s = Search(using=elastic, index='hierarchy') \
    .query('ids', values=[f'{hierarchy_id}_{agent}'])
  response = s[:1].execute()

  if response.hits.total == 1:
    return response.hits.hits[0]['_source']
  else:
    return None


def agent_communities(agent, hierarchy_id=None):
  """Array of communities for an agent from the hierarchy."""

  if hierarchy_id == None:
    hierarchy_id = latest_hierarchy()

  s = Search(using=elastic, index='hierarchy') \
    .query('ids', values=[f'{hierarchy_id}_{agent}'])

  response = s[0:1].execute()
  if response.hits.total == 1:
    return response.hits.hits[0]['_source']['community_id']
  else:
    return []


def agent_installs(agent, start, end):
  """Installations created by an agent."""

  s = Search(using=elastic, index='installs') \
    .query('term', doctype='install') \
    .query('term', agent_id=agent) \
    .query('range', opened={ 'gte': start, 'lt': end })

  installs = []
  for hit in s.scan():
    installs.append(hit.to_dict())

  return installs


def client_dos(communities=None, hierarchy_id=None, agent_id=None, date=None):
  """Client docs from array of communities or agent_id.

  An agent_id is used to get a list of communities from the hierarchy.
  """

  if communities == None:
    communities = agent_communities(agent_id, hierarchy_id=hierarchy_id)
  if date is None:
    date = dt.datetime.today().strftime('%Y-%m-%d')

  s = Search(using=elastic, index='people') \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', opened={ 'lt': date }),
      Q('bool', should=[
        Q('term', open=True),
        Q('range', closed={ 'gt': date })
      ]),
      Q('bool', should=[
        ~Q('exists', field='pos_open'),
        Q('range', pos_closed={ 'lt': date })
      ])
    ])

  clients = []
  for hit in s.scan():
    clients.append(hit.to_dict())

  return clients


def client_ids(communities=None, hierarchy_id=None, agent_id=None, date=None):
  """Client ids from array of communities or agent_id.

  An agent_id is used to get a list of communities from the hierarchy.
  """

  if communities == None:
    communities = agent_communities(agent_id, hierarchy_id=hierarchy_id)
  if date is None:
    date = dt.datetime.today().strftime('%Y-%m-%d')

  s = Search(using=elastic, index='people') \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', opened={ 'lt': date }),
      Q('bool', should=[
        Q('term', open=True),
        Q('range', closed={ 'gt': date })
      ]),
      Q('bool', should=[
        ~Q('exists', field='pos_open'),
        Q('range', pos_closed={ 'lt': date })
      ])
    ]) \
    .source([ 'person_id' ])

  client_ids = []
  for hit in s.scan():
    client_ids.append(hit.person_id)

  return client_ids


def codes(start, end, communities=None, hierarchy_id=None, agent_id=None):
  """Codes from array of communities or agent_id.

  An agent_id is used to get a list of communities from the hierarchy.
  """

  if communities == None:
    communities = agent_communities(agent_id, hierarchy_id=hierarchy_id)

  s = Search(using=elastic, index='codes') \
    .query('term', doctype='code') \
    .query('terms', to__community__community_id=communities) \
    .query('range', datetime={
      'gte': start,
      'lt': end
    })

  codes = []
  for hit in s.scan():
    codes.append(hit.to_dict())

  return codes