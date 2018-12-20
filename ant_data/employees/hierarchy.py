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
from ant_data.communities import communities as _communities
from ant_data.employees import employees
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def index(hierarchy_id, country, cm, agents, coordinators, supervisors):
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


# FIXME: latest hierarchy id?
def info(hierarchy_id, agent):
  s = Search(using=elastic, index='hierarchy') \
    .query('ids', values=[f'{hierarchy_id}_{agent}'])
  response = s[:1].execute()

  if response.hits.total == 1:
    return response.hits.hits[0]['_source']
  else:
    return None


# FIXME: latest hierarchy id?
def communities(hierarchy_id, agent):
  s = Search(using=elastic, index='hierarchy') \
    .query('ids', values=[f'{hierarchy_id}_{agent}'])

  response = s[0:1].execute()
  if response.hits.total == 1:
    return response.hits.hits[0]['_source']['community_id']
  else:
    return []


def installs(agent, start, end):
  s = Search(using=elastic, index='installs') \
    .query('term', doctype='install') \
    .query('term', agent_id=agent) \
    .query('range', opened={ 'gte': start, 'lt': end })

  installs = []
  for hit in s.scan():
    installs.append(hit.to_dict())

  return installs


# FIXME: latest hierarchy id?
def clients(communities, date=None):
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


# FIXME: latest hierarchy id?
def codes(communities, start, end):
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


if __name__=='__main__':
  index('Guatemala')
  # index('Colombia') FIXME: