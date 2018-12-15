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

import pandas as pd
from pandas import concat,DataFrame


def agents():
  path = '../google/roster_agents.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  
  return pd.read_csv(filepath, index_col='agent_id')

def supervisors():
  path = '../google/roster_supervisors.csv'
  filepath = pkg_resources.resource_filename(__name__, path)

  return pd.read_csv(filepath, index_col='supervisor_id')

def coordinators():
  path = '../google/roster_coordinators.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  
  return pd.read_csv(filepath, index_col='coordinator_id')
