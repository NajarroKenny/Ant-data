"""
Sync log
==========================
Fetches the sync log stored in Elasticsearch

- Create date:  2018-12-11
- Update date:  2018-12-26
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
- v1.1: Removed country dependency
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def search(agent_id=None, person_id=None, f=None):
  """Searches the latest sync date of each system associated with an agent or 
  shopkeeper, but NOT both. If no ID parameter is passed, it fetches the entire 
  sync log.

  Args:
    agent_id (str, optional): ID of Agent for whom to fetch the last sync date.
      Defaults to None.
    person_id (str, optional): ID of the Shopkeeper for whom to fetch the last
      sync date. Defaults to None.
    f (list, optional): List of additional filters to pass to the query. The
      list is composed of Elasticserach DSL Q boolean objects. Defaults to None 

  Returns:
    elasticsearch_dsl scan object
  """

  if agent_id is not None and person_id is not None:
    raise Exception(f'Cannot pass both agent_id and person_id')

  s = Search(using=elastic, index='sync_log')

  if agent_id is not None:
    agent_id = agent_id if isinstance(agent_id, list) else [agent_id]
    s = s.filter('terms', agent_id=agent_id)

  elif person_id is not None:
    person_id = person_id if isinstance(person_id, list) else [person_id]
    s = s.filter('terms', person_id=person_id)

  if f is not None:
    s = s.query('bool', filter=f)

  return s.scan()


def df(agent_id=None, person_id=None, f=None):
  """Returns a DataFrame with the latest sync date of each system associated
  with an agent or shopkeeper, but NOT both. If no ID is passed, it returns the
  entire sync log.

  Args:
    agent_id (str, optional): ID of Agent for whom to fetch the last sync date.
      Defaults to None.
    person_id (str, optional): ID of the Shopkeeper for whom to fetch the last
      sync date. Defaults to None.
    f (list, optional): List of additional filters to pass to the query. The
      list is composed of Elasticserach DSL Q boolean objects. Defaults to None 

  Returns:
    DataFrame: Pandas DataFrame with index = 'system_id' and the following 
    columns = 'country', 'instance_type', 'agent_id', 'person_id', 'sync_date'
  """
  if agent_id is not None and person_id is not None:
    raise Exception(f'Cannot pass both agent_id and person_id')

  response = search(agent_id, person_id, f)

  obj = {}

  for hit in response:
    try:
      person_id = hit.person_id
    except:
      person_id = None

    try:
      agent_id = hit.agent_id
    except:
      agent_id = None

    try:
      instance_type = hit.instance_type
    except:
      instance_type = None

    obj[hit.system_id] = {
      'country': hit.country,
      'instance_type': instance_type,
      'agent_id': agent_id,
      'person_id': person_id,
      'sync_date': hit.sync_date
      }

  df = DataFrame.from_dict(
    obj, orient='index', columns=[
      'country','instance_type', 'agent_id', 'person_id', 'sync_date'
    ]
  )

  if df.empty:
    return df

  df.index.name = 'system_id'
  df = df.sort_index()

  return df

