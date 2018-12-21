"""
Roster
============================
Loads the AT & ATR Roster CSV files into Pandas DataFrames

- Create date:  2018-12-14
- Update date:  2018-12-14
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
import pkg_resources

import numpy as np
import pandas as pd
from pandas import concat,DataFrame


def agents(): # FIXME:P1 country parameter FIXME:P1 information should be in ES
  """Array of roster information for agents."""

  path = '../static/roster_agents.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  df = pd.read_csv(filepath, index_col='agent_id').replace(np.nan, '')
  return df

def supervisors(): # FIXME:P1 country parameter FIXME:P1 information should be in ES
  """Array of roster information for supervisors."""

  path = '../static/roster_supervisors.csv'
  filepath = pkg_resources.resource_filename(__name__, path)

  return pd.read_csv(filepath, index_col='supervisor_id')

def coordinators(): # FIXME:P1 country parameter FIXME:P1 information should be in ES
  """Array of roster information for coordinators."""

  path = '../static/roster_coordinators.csv'
  filepath = pkg_resources.resource_filename(__name__, path)

  return pd.read_csv(filepath, index_col='coordinator_id')

def person(id): # FIXME:P1 information should be in ES
  """Find roster information for an in."""

  path = '../static/roster_agents.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  agents = pd.read_csv(filepath, index_col='agent_id')

  path = '../static/roster_coordinators.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  coordinators = pd.read_csv(filepath, index_col='coordinator_id')

  path = '../static/roster_supervisors.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  supervisors = pd.read_csv(filepath, index_col='supervisor_id')

  level = ''
  if id in agents.index:
    level = 'agent'
    agent = agents.loc[id]
    coordinator = agent['coordinator_id']
    supervisor = agent['supervisor_id']
  elif id in coordinators.index:
    level = 'coordinator'
    coordinator = coordinators.loc[id]
    supervisor = coordinator['supervisor_id']
    agent = agents[agents['coordinator_id'] == id].index.values
  elif id in supervisors.index:
    level = 'supervisor'
    supervisor = supervisors.loc[id]
    agent = agents[agents['supervisor_id'] == id].index.values
    coordinator = coordinators[coordinators['supervisor_id'] == id].index.values
  else:
    return None

  return {
    'level': level,
    'agent': agent,
    'coordinator': coordinator,
    'supervisor': supervisor
  }
