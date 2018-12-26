"""
Agent Mapping
==========================
Script that returns a dataframe with the agent mapping read from Elasticsearch

- Create date:  2018-12-26
- Update date:  2018-12-26
- Version:      1.1

Notes:
==========================        
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search
from pandas import DataFrame

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def df(f=None):
  """Retrieves the agent mapping from Elasticsearch as a Dataframe
  
  Args:
    f (list, optional): List of additional filters to pass to the query. The
      list is composed of Elasticserach DSL Q boolean objects. Defaults to none

  Returns:
    Pandas Dataframe with index = and columns = []
  """
  s = Search(using=elastic, index=CONFIG['ES']['AGENT_MAPPING'])
    
  if f is not None:
    s = s.query('bool', filter=f)

  hits = s[:10000].execute()
  obj = {}

  for hit in hits:
    obj[hit.new_id] = hit.old_id
  
  df = DataFrame.from_dict(obj, orient='index')
  df = df.rename(columns={0:'old_id'})
  df.index.name = 'new_id'
  
  return df