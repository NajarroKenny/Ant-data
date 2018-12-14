from pandas import DataFrame
import pandas as pd
import pkg_resources

def community_master():
  path = '../google/community_master.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  cm = pd.read_csv(filepath, index_col='concat')

  return cm