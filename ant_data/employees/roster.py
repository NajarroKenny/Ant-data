"""
Roster
============================
Loads the AT & ATR Roster CSV files into Pandas DataFrames

- Create date:  2018-12-14
- Update date:  2018-12-27
- Version:      1.1

Notes:
============================
- v1.0: Initial version
- v1.1: Fetch roster from Elasticsearch instead of CSV files
"""
import configparser

from elasticsearch_dsl import Search
import numpy as np
from pandas import DataFrame

from ant_data import elastic, ROOT_DIR
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def agents(country):
  """Array of roster information for agents.
  
  Args:
    country (str): Guatemala or Colombia

  Returns:
    DataFrame: Pandas DataFrame with index = agent_id and columns = [
      'coordinator', 'coordinator_id', 'coordinator_ref', 'coordinator_ref_id', 
      'country', 'department', 'municipality', 'name', 'phone', 'role', 
      'role_id', 'start_date', 'supervisor', 'supervisor_id', 'system_id']
  """
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  s = Search(using=elastic, index=CONFIG['ES']['ROSTER']) \
    .query('term', country=country) \
    .query('terms', role_id=['at', 'atr', 'it'])

  res = s[:10000].execute()

  hits = [x.to_dict() for x in res.hits]
  
  df = DataFrame(hits)
  df = df.set_index('agent_id').replace(np.nan, '')
  
  return df


def coordinators(country):
  """Array of roster information for coordinators.
  
  Args:
    country (str): Guatemala or Colombia

  Returns:
    DataFrame: Pandas DataFrame with index = coordinator_id and columns = [
      'country', 'name', 'phone', 'region', 'role', 'role_id', 'start_date',
      'supervisor', 'supervisor_id', 'system_id']
  """
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  s = Search(using=elastic, index=CONFIG['ES']['ROSTER']) \
    .query('term', country=country) \
    .query('terms', role_id=['coordinator'])

  res = s[:10000].execute()

  hits = [x.to_dict() for x in res.hits]
  
  df = DataFrame(hits)
  df = df.set_index('coordinator_id')
  
  return df


def supervisors(country): 
  """Array of roster information for supervisors.
  
  Args:
    country (str): Guatemala or Colombia

  Returns:
    DataFrame: Pandas DataFrame with index = supervisor_id and columns = [
     'country', 'manager', 'manager_id', 'name', 'phone', 'region', 'role', 
     'role_id', 'start_date', 'system_id']
  """
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  s = Search(using=elastic, index=CONFIG['ES']['ROSTER']) \
    .query('term', country=country) \
    .query('terms', role_id=['supervisor'])

  res = s[:10000].execute()

  hits = [x.to_dict() for x in res.hits]
  
  df = DataFrame(hits)
  df = df.set_index('supervisor_id')
  
  return df


def person(country, id):
  """Find roster information for an individual.
  
  Args:
    country (str): Guatemala or Colombia
    id (str): Employee ID
  
  Returns:
    dictionary: person summary dictionary with keys = 'level', 'agent', 
      'coordinator', 'supervisor'. Depending on the level, each of the values 
      might be a string, a list of strings, or a Pandas Series. Returs None if 
      no match is found.
  """
  df_agents = agents(country)
  df_coordinators = coordinators(country)
  df_supervisors = supervisors(country)

  if id in df_agents.index:
    level = 'agent'
    agent = df_agents.loc[id]
    coordinator = agent['coordinator_id']
    supervisor = agent['supervisor_id']
  elif id in df_coordinators.index:
    level = 'coordinator'
    coordinator = df_coordinators.loc[id]
    supervisor = coordinator['supervisor_id']
    agent = df_agents[df_agents['coordinator_id'] == id].index.values
  elif id in df_supervisors.index:
    level = 'supervisor'
    supervisor = df_supervisors.loc[id]
    agent = df_agents[df_agents['supervisor_id'] == id].index.values
    coordinator = df_coordinators[df_coordinators['supervisor_id'] == id].index.values
  else:
    return None

  return {
    'level': level,
    'agent': agent,
    'coordinator': coordinator,
    'supervisor': supervisor
  }
