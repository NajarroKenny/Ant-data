"""
Community Master
============================
Loads the Community Master CSV file into a Pandas DataFrame

- Create date:  2018-12-14
- Update date:  2018-12-14
- Version:      1.0

Notes:
============================      
- v1.0: Initial version
"""
import pkg_resources

import pandas as pd
from pandas import DataFrame


def community_master():
  path = '../google/community_master.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  cm = pd.read_csv(filepath, index_col='concat')

  return cm