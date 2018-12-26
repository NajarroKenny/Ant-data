"""
Community Shopkeepers
==========================
Retrieves list of shopkeepers given a community list

- Create date:  2018-12-18
- Update date:  2018-12-26
- Version:      1.1

Notes:
==========================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def shopkeepers(country, community_list, f=None):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
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