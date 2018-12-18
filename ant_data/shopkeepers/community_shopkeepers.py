"""
Community Shopkeepers
==========================
Retrieves list of shopkeepers given a community list

- Create date:  2018-12-18
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def shopkeepers(country, community_list, f=None):
  s = Search(using=elastic, index='people') \
    .query('term', country=country) \
    .query('term', doctype='client') \
    .query('term', pos_open=True) \
    .filter('terms', community__community_id=community_list)

  if f is not None:
    s = s.query('bool', filter=f)

  shopkeepers = list()

  for hit in s.scan():
    shopkeepers.append(hit.person_id)

  return sorted(shopkeepers)