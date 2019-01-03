
"""
Index hierarchy
==========================
Indexes roster and community master information in hierarchy index

- Create date:  2018-12-15
- Update date:  2019-01-02
- Version:      1.3

Notes:
==========================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
- v1.2: Add full roster information 
- v1.3: Rename 'agent' functions to 'employee'. Add docstrings. General clean up
"""
import configparser
import datetime as dt

from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import bulk
from pandas import DataFrame, Timestamp

from ant_data import elastic, ROOT_DIR
from ant_data.shared.helpers import local_date_dt
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def index(hierarchy_id, country, cm, agents, coordinators, supervisors):
  """Indexes a hierarchy in Elasticsearch assigning it a hierarchy_id based on
  the current date. The hierarcy is country-specific.
  
  Args:
    hierarchy_id (str): ISO8601 date to serve as label for the hierarchy.
    country (str): Guatemala or Colombia. 
    cm (DataFrame): Country-specific community master from Google Sheets
    agents (DataFrame): Country-specific agent list from ES roster
    coordinators (DataFrame): Country-specific coordinator list from ES roster.
    supervisors (DataFrame): Country-specific supervisor list from ES roster.
  
  Returns:
    None.
  """
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
        "department": agent['department'],
        "municipality": agent['municipality'],
        "name": agent['name'],
        "phone": agent['phone'],
        "role": agent['role'],
        "doctype": agent['role_id'],
        "agent_id": key,
        "start_date": agent['start_date'],
        "coordinator": agent['coordinator'],
        "coordinator_id": agent['coordinator_id'],
        "supervisor": agent['supervisor'],
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
      "name": coordinator['name'],
      "phone": coordinator['phone'],
      "region": coordinator['region'],
      "role": coordinator['role'],
      "doctype": coordinator['role_id'],
      "agent_id": agent_ids,
      "start_date": coordinator['start_date'],
      "coordinator_id": key,
      "supervisor": coordinator['supervisor'],
      "supervisor_id": coordinator['supervisor_id'],
      "system_id": coordinator['system_id'],
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
        "manager": supervisor['manager'],
        "manager_id": supervisor['manager_id'],
        "name": supervisor['name'],
        "agent_id": agent_ids,
        "phone": supervisor['phone'],
        "region": supervisor['region'],
        "role": supervisor['role'],
        "start_date": supervisor['start_date'],
        "system_id": supervisor['system_id'],
        "coordinator_id": coordinator_ids,
        "supervisor_id": key,
        "community_id": community_ids
      }
    docs.append(doc)

  bulk(elastic, docs)


def hierarchy_list(country):
  """Returns the last 100 hierarchy_ids from the hierarchy index.
  
  Args:
    country (str): Guatemala or Colombia.
    
  Returns:
    list: List of string hierarchy_ids.
  """

  s = Search(using=elastic, index=CONFIG['ES']['HIERARCHY']) \
    .query('term', country=country)

  s.aggs \
    .bucket('hierarchy_id', 'terms', field='hierarchy_id', order={ '_key': 'desc' }, size=100)

  response = s[:0].execute()

  ids = [x.key for x in response.aggs.hierarchy_id]

  return ids


def latest_hierarchy(date=None):
  """Returns the latest hierarchy indexed in Elasticsearch. 
    
  Args:
    date (str, optional): ISO8601 date string to serve as end date. Defaults
      to None, in which case it searches all hierarchies.
    
  Returns:
    str: Latest hierarchy. If not found, returns a blank string.
  """

  s = Search(using=elastic, index=CONFIG['ES']['HIERARCHY'])

  if date is not None:
    s = s.query('range', hierarchy_id={'lte':date}) #FIXME:P2 Revise when new hierarchy is in Couch

  s.aggs \
    .bucket('hierarchy_id', 'terms', field='hierarchy_id', order={ '_key': 'desc' }, size=1)

  response = s[:0].execute()

  ids = [x.key for x in response.aggs.hierarchy_id]

  return ids[0] if len(ids) == 1 else ''


def employee_info(employee_id, hierarchy_id=None, date=None):
  """Returns sthe employee information from a specified hierarchy
  
  Args: 
    employee_id (str): Agent, Coordinantor, or Supervisor ID.
    hierarchy_id (str, optional): ID of the hierarchy to use. Defaults to None, 
      in which case the latest hierarchy is used.
    date (str, optional): Date to use for the latest hierarchy if the 
      hierarchy_id is not passed. Defaults to None, in which case the latest 
      hierarchy is used.

  Returns:
    dict: Employee information dictionary with keys = ['hierarchy_id', 'country', 
      'doctype', 'agent_id', 'coordinator_id', 'supervisor_id', 'community_id'].
      Defaults to None if no information is found.
  """
  if hierarchy_id == None:
    hierarchy_id = latest_hierarchy(date)

  s = Search(using=elastic, index=CONFIG['ES']['HIERARCHY']) \
    .query('ids', values=[f'{hierarchy_id}_{employee_id}'])
  response = s[:1].execute()

  if response.hits.total == 1:
    return response.hits.hits[0]['_source']
  else:
    return None


def employee_communities(employee_id, hierarchy_id=None, date=None):
  """Employee community list from the hierarchy.

  Args:
    employee_id (str): Agent, Coordinantor, or Supervisor ID.
    hierarchy_id (str, optional): ID of the hierarchy to use. Defaults to None, 
      in which case the latest hierarchy is used.
    date (str, optional): Date to use for the latest hierarchy if the 
      hierarchy_id is not passed. Defaults to None, in which case the latest 
      hierarchy is used.
  
  Returns:
    list: Employee's community list.  
  """  
  if hierarchy_id == None:
    hierarchy_id = latest_hierarchy(date)

  s = Search(using=elastic, index=CONFIG['ES']['HIERARCHY']) \
    .query('ids', values=[f'{hierarchy_id}_{employee_id}'])

  response = s[0:1].execute()
  if response.hits.total == 1:
    return response.hits.hits[0]['_source']['community_id']
  else:
    return []


def agent_installs(agent_id, start, end, pos=False):
  """Installations created by an agent.

  Args:
    agent_id (str): Agent ID.
    start (str): ISO8601 date interval start.
    end (str): ISO8601 date interval end.
    pos (bool, optional): Flag to only search for Shopkeepers. Defaults to False.
  
  Returns:
    list: List with install docs as stored in Elasticsearch. Each install doc is
      a dictionary with the same fields as the original doc in Elasticsearch.
  """
  s = Search(using=elastic, index=CONFIG['ES']['INSTALLS']) \
    .query('term', doctype='install') \
    .query('term', agent_id=agent_id) \
    .query('range', opened={ 'gte': start, 'lt': end })

  if pos:
    s = s.query('term', system_type='pos')

  installs = [x.to_dict() for x in s.scan()]
  
  return installs


def agent_install_count(agent_id, start, end, pos=False):
  """Agent installation count.
  
  Args:
    agent_id (str): Agent ID.
    start (str): ISO8601 date interval start.
    end (str): ISO8601 date interval end.
    pos (bool, optional): Flag to only search for Shopkeepers. Defaults to False.
  
  Returns:
    int: Agent install count.
  """
  s = Search(using=elastic, index=CONFIG['ES']['INSTALLS']) \
    .query('term', doctype='install') \
    .query('term', agent_id=agent_id) \
    .query('range', opened={ 'gte': start, 'lt': end })

  if pos:
    s = s.query('term', system_type='pos')

  return s[:0].execute().hits.total


def client_docs(
  communities=None, country=None, date=None, employee_id=None, hierarchy_id=None
  ):
  """Client docs for a community list or employee_id. Either a community list and
  country/date, or an employee_id must be provided.

  Args:
    communities (list, optional): Community list to retrieve the client docs. 
      Defaults to None.
    country (str, optional): Guatemala or Colombia. Mandatory if a community 
      list is passed with NO date. Defaults to None.
    date (str, optional): ISO8601 date. Mandatory if a community list
      is passed with NO country. Defaults to None, in which case the country
      is used to determine its local date.
    employee_id (str, optional): Employee ID used to retrieve the community 
      list. Defaults to None. 
    hierarchy_id (str, optional): ID of the hiearchy to use to retrieve the 
      community list. Defaults to None.

  Returns:
    list: Dictionary list of all client docs for the given employee. The 
      dictionary keys match the fields in the Elasticsearch client docs.
  """
  if communities is None:
    country = employee_info(employee_id)['country']
    communities = employee_communities(employee_id, hierarchy_id, date)
  
  else:
    communities = communities if isinstance(communities, list) else [communities]

  if date is None:
    date = local_date_dt(country)

  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', kingo_opened={ 'lte': date }), #TODO:P2 check this makes sense
      Q('bool', should=[ 
        Q('term', kingo_open=True),
        Q('range', kingo_closed={ 'gt': date })
      ])
    ])

  clients = [x.to_dict() for x in s.scan()]
  
  return clients


def client_ids(
  communities=None, country=None, date=None, employee_id=None, hierarchy_id=None
  ):
  """Client ids for a community list or employee_id. Either a community list and
  country/date, or an employee_id must be provided.

  Args:
    communities (list, optional): Community list to retrieve the client docs. 
      Defaults to None.
    country (str, optional): Guatemala or Colombia. Mandatory if a community 
      list is passed with NO date. Defaults to None.
    date (str, optional): ISO8601 date. Mandatory if a community list
      is passed with NO country. Defaults to None, in which case the country
      is used to determine its local date.
    employee_id (str, optional): Employee ID used to retrieve the community 
      list. Defaults to None. 
    hierarchy_id (str, optional): ID of the hiearchy to use to retrieve the 
      community list. Defaults to None.

  Returns:
    list: List of string Client IDs.
  """
  if communities == None:
    country = employee_info(employee_id)['country']
    communities = employee_communities(employee_id, hierarchy_id, date)
  
  else:
    communities = communities if isinstance(communities, list) else [communities]

  if date is None:
    date = local_date_dt(country)

  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', kingo_opened={ 'lte': date }), #TODO:P2 check this makes sense
      Q('bool', should=[
        Q('term', kingo_open=True),
        Q('range', kingo_closed={ 'gt': date })
      ])
    ]) \
    .source([ 'person_id' ])

  client_ids = [x.person_id for x in s.scan()]
  
  return client_ids


def shopkeeper_ids(
  communities=None, country=None, date=None, employee_id=None, hierarchy_id=None
  ):
  """Client ids for a community list or employee_id. Either a community list and
  country/date, or an employee_id must be provided.

  Args:
    communities (list, optional): Community list to retrieve the client docs. 
      Defaults to None.
    country (str, optional): Guatemala or Colombia. Mandatory if a community 
      list is passed with NO date. Defaults to None.
    date (str, optional): ISO8601 date. Mandatory if a community list
      is passed with NO country. Defaults to None, in which case the country
      is used to determine its local date.
    employee_id (str, optional): Employee ID used to retrieve the community 
      list. Defaults to None. 
    hierarchy_id (str, optional): ID of the hiearchy to use to retrieve the 
      community list. Defaults to None.

  Returns:
    list: List of string Shopkeeper IDs.
  """
  if communities == None:
    country = employee_info(employee_id)['country']
    communities = employee_communities(employee_id, hierarchy_id, date)
  
  else:
    communities = communities if isinstance(communities, list) else [communities]

  if date is None:
    date = local_date_dt(country)

  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', pos_opened={ 'lte': date }), #TODO:P2 check this makes sense
      Q('bool', should=[
        Q('term', pos_open=True),
        Q('range', pos_closed={ 'gt': date })
      ])
    ]) \
    .source([ 'person_id' ])

  shopkeeper_ids = [x.person_id for x in s.scan()]

  return shopkeeper_ids


def shopkeeper_docs(
  communities=None, country=None, date=None, employee_id=None, hierarchy_id=None
  ):
  """Client docs for a community list or employee_id. Either a community list and
  country/date, or an employee_id must be provided.

  Args:
    communities (list, optional): Community list to retrieve the client docs. 
      Defaults to None.
    country (str, optional): Guatemala or Colombia. Mandatory if a community 
      list is passed with NO date. Defaults to None.
    date (str, optional): ISO8601 date. Mandatory if a community list
      is passed with NO country. Defaults to None, in which case the country
      is used to determine its local date.
    employee_id (str, optional): Employee ID used to retrieve the community 
      list. Defaults to None. 
    hierarchy_id (str, optional): ID of the hiearchy to use to retrieve the 
      community list. Defaults to None.

  Returns:
    list: Dictionary list of all shopkeeper docs for the given employee. The 
      dictionary keys match the fields in the Elasticsearch shopkeeper docs.
  """
  if communities is None:
    country = employee_info(employee_id)['country']
    communities = employee_communities(employee_id, hierarchy_id, date)
  
  else:
    communities = communities if isinstance(communities, list) else [communities]

  if date is None:
    date = local_date_dt(country)

  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', doctype='client') \
    .query('terms', community__community_id=communities) \
    .query('bool', must=[
      Q('range', pos_opened={ 'lte': date }), #TODO:P2 check this makes sense
      Q('bool', should=[
        Q('term', pos_open=True),
        Q('range', pos_closed={ 'gt': date })
      ])
    ])

  shopkeepers = [x.to_dict() for x in s.scan()]
  
  return shopkeepers


def codes(start, end, communities=None, employee_id=None, hierarchy_id=None,):
  """Codes from a community list or employee_id.

  Args:
    start (str): ISO8601 date interval start date.
    end (str): ISO8601 date interval end date.
    communities (list, optional): Community list to retrieve the client docs. 
      Defaults to None.
    employee_id (str, optional): Employee ID used to retrieve the community 
      list. Defaults to None. 
    hierarchy_id (str, optional): ID of the hiearchy to use to retrieve the 
      community list. Defaults to None.
  
  Returns:
    list: Dictionary list of all code docs for the given employee. The 
      dictionary keys match the fields in the Elasticsearch codes docs.
  """

  if communities is None:
    communities = employee_communities(employee_id, hierarchy_id)
  
  else:
    communities = communities if isinstance(communities, list) else [communities]

  s = Search(using=elastic, index=CONFIG['ES']['CODES']) \
    .query('term', doctype='code') \
    .query('terms', to__community__community_id=communities) \
    .query('range', datetime={
      'gte': start,
      'lt': end
    })

  codes = [x.to_dict() for x in s.scan()]
  
  return codes