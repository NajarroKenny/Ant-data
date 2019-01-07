"""
Systems Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on systems open by model. The 'open' count is done
in five different ways: 'start', 'end', 'average', and 'weighted'

- Create date:  2018-12-06
- Update date:  2018-12-26
- Version:      1.4

Notes:
==========================
- v1.0: Uses min_doc_count parameter and pre-populates de obj dictionary
        with all date x model combinations to guarantee dates are not sparse.
- v1.1: Fixed bug when key didn't exit in df_open_now
- v1.3: Major clean up, rewrite open calculations, remove doctype filtering
- v1.4: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, MultiIndex, offsets, Series, Timestamp

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None):
  s = Search(using=elastic, index=CONFIG['ES']['INSTALLS']) \
    .query('term', system_type='kingo')

  if end is not None and start is not None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('range', opened={ 'lt': end }),
        Q('bool', should=[
          ~Q('exists', field='closed'),
          Q('range', closed={ 'gte': start })
        ])
      ]
    )
  elif end is not None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('range', opened={ 'lt': end })
      ]
    )
  elif start is not None:
    s = s.query(
      'bool', filter=[
        Q('term', country=country),
        Q('bool', should=[
          ~Q('exists', field='closed'),
          Q('range', closed={ 'gte': start })
        ])
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('models', 'terms', field='version')

  return s[:0].execute()


def l(country, start=None, end=None, f=None):
  response = search(country, start=start, end=end, f=f)

  return sorted([x.key for x in response.aggs.models.buckets])