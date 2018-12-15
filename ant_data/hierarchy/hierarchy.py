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
from elasticsearch.helpers import bulk
from pandas import DataFrame, Timestamp

from ant_data import elastic
from ant_data.communities import communities
from ant_data.people import people
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def index(country):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  cm = communities.community_master_ids()
  agents = people.agents()
  coordinators = people.coordinators()
  supervisors = people.supervisors()

  docs = []

  for row in agents.iterrows():
    (key, agent) = row
    doc = {
        "_index": "hierarchy",
        "_type": "_doc",
        "_id": key,
        "country": country,
        "doctype": agent['role_id'],
        "agent_id": key,
        "coordinator_id": agent['coordinator_id'],
        "supervisor_id": agent['supervisor_id'],
        "communities": cm[cm['at']==key]['community_id'].tolist()
      }
    docs.append(doc)

  for row in coordinators.iterrows():
    (key, coordinator) = row
    agent_ids = agents[agents['coordinator_id']==key].index.tolist()
    community_ids = cm[cm['at'].isin(agent_ids)]['community_id'].tolist()
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
    community_ids = cm[cm['at'].isin(agent_ids)]['community_id'].tolist()
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
