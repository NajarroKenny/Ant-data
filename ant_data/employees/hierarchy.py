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
from ant_data.people import people
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def index(country):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  cm = _communities.community_master_ids()
  agents = people.agents()
  coordinators = people.coordinators()
  supervisors = people.supervisors()

  docs = []

  for row in agents.iterrows():
    (key, agent) = row
    if agent['coordinator_ref_id'] == '':
      community_ids = cm[cm['agent_id']==key]['community_id'].tolist()
    else:
      agent_ids = agents[agents['coordinator_id']==agent['coordinator_ref_id']].index.tolist()
      community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
    doc = {
        "_index": "hierarchy",
        "_type": "_doc",
        "_id": key,
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
    community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
    doc = {
      "_index": "hierarchy",
      "_type": "_doc",
      "_id": key,
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
    coordinator_ids = coordinators[coordinators['supervisor_id']==key].index.tolist()
    community_ids = cm[cm['agent_id'].isin(agent_ids)]['community_id'].tolist()
    doc = {
        "_index": "hierarchy",
        "_type": "_doc",
        "_id": key,
        "country": country,
        "doctype": supervisor['role_id'],
        "agent_id": agent_ids,
        "coordinator_id": coordinator_ids,
        "supervisor_id": key,
        "community_id": community_ids
      }
    docs.append(doc)


  bulk(elastic, docs)


def communities(agent):
  s = Search(using=elastic, index='hierarchy') \
    .query('ids', values=[agent])

  response = s[0:1].execute()
  if response.hits.total == 1:
    return response.hits.hits[0]['_source']['community_id']
  else:
    return []


def clients(agent, agent_type, date=None):
  if date is None:
    date = dt.datetime.today().strftime('%Y-%m-%d')
  s = Search(using=elastic, index='people') \
    .query('term', doctype='client') \
    .query('term', **{ f'community.employees.{agent_type}.agent_id': agent }) \
    .query('bool', should=[
      Q('term', open=True),
      Q('range', closed={ 'gte': date })
    ])

  clients = []
  for hit in s.scan():
    clients.append(hit)

  return clients


def codes(agent, start, end):
  c = communities(agent)

  s = Search(using=elastic, index='codes') \
    .query('term', doctype='code') \
    .query('terms', to__community__community_id=c) \
    .query('range', datetime={
      'gte': start,
      'lt': end
    })

  codes = []
  for hit in s.scan():
    codes.append(hit)

  return codes


if __name__=='__main__':
  index('Guatemala')
  # index('Colombia') FIXME: