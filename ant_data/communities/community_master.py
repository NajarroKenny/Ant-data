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


import pandas as pd
from pandas import DataFrame


def community_master():
  path = '../google/community_master.csv'
  filepath = pkg_resources.resource_filename(__name__, path)
  cm = pd.read_csv(filepath, index_col='concat')

  return cm


def cm_ids():
  cm = community_master()

  ids = []
  scores = []
  second_scores = []
  communities = []
  municipalities = []
  departments = []

  for row in cm.iterrows():
    (key, data) = row
    s = Search(using=elastic, index='communities') \
      .query('bool', should=[
        Q('match', community=data['community']),
        Q('match', municipality__text=data['municipality']),
        Q('match', department__text=data['department'])
      ])
    response = s[0:5].execute()
    # print(data['community'], data['municipality'], data['department'])
    # for hit in response.hits.hits:
    #   print(hit['_score'], hit['_source']['community'], hit['_source']['municipality'], hit['_source']['department'])

    ids.append(response.hits.hits[0]['_source']['community_id'])
    scores.append(response.hits.hits[0]['_score'])
    second_scores.append(response.hits.hits[1]['_score'])
    communities.append(response.hits.hits[0]['_source']['community'])
    municipalities.append(response.hits.hits[0]['_source']['municipality'])
    departments.append(response.hits.hits[0]['_source']['department'])

  cm['community_id'] = ids
  cm['ant_community'] = communities
  cm['ant_municipality'] = municipalities
  cm['ant_department'] = departments
  cm['scores'] = scores
  cm['second_scores'] = second_scores
  cm['percentage'] = round(100 * cm['second_scores'] / cm['scores'])

  cm = cm.reset_index()
  cm = cm[['community_id', 'concat', 'scores', 'second_scores', 'percentage', 'ant_community', 'community', 'ant_municipality', 'municipality', 'ant_department', 'department', 'at', 'cs', 'ss', 'clients']]
  cm.to_csv('community_master_ids.csv', sep=',')

  return cm
