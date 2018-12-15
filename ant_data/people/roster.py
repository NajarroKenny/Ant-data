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


def at():
  path = '../google/roster_at.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  at = pd.read_csv(filepath, index_col='correo')
  at.index.name = 'agent_id'

  return at

def atr():
  path = '../google/roster_atr.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  atr = pd.read_csv(filepath, index_col='correo')
  atr.index.name = 'agent_id'

  return atr

def roster():
  return concat([at(),atr()])