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
from elasticsearch_dsl import Search, Q

from ant_data import elastic
from ant_data.people import people as p


import pandas as pd
from pandas import DataFrame


def df():
  path = '../static/community_master.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  cm = pd.read_csv(filepath, index_col='community_id')

  return cm


def df_join_ids():
  cm = df()

  ids = []
  for row in cm.iterrows():
    (key, data) = row
    s = Search(using=elastic, index='communities') \
      .query('bool', must=[
        Q('term', community__raw=data['community'].title()),
        Q('term', municipality=data['municipality'].title()),
        Q('term', department=data['department'].title())
      ])
    response = s[0:1].execute()

    if response.hits.total == 1:
      ids.append(response.hits.hits[0]['_source']['community_id'])
    else:
      ids.append('')

  cm['__community_id'] = ids
  cm = cm.reset_index()
  cm = cm.rename(columns={ 'at': 'agent_id', 'community_id': '_community_id', '__community_id': 'community_id' })
  cm = cm[['community_id', '_community_id', 'community', 'municipality', 'department', 'agent_id']]

  cm.to_csv('community_master_join.csv', sep=',')

  return cm