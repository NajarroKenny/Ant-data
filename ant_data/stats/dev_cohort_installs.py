import configparser
from copy import deepcopy

from elasticsearch_dsl import Search, Q
import numpy as np
from pandas import DataFrame, date_range

from ant_data import elastic, ROOT_DIR
from ant_data.people import people_open
from ant_data.stats import dev_active_systems__date


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start, end, f=None, interval='month'):
  s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
    .query('term', country=country) \
    .query('range', kingo_opened={
      "gte": start,
      "lt": end
    })

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket('installations', 'nested', path='installations') \
    .bucket('cohort', 'filter', filter=Q('bool', must=[
      Q('range', installations__opened={
        "gte": start,
        "lt": end
      }),
      Q('bool', should=[
        ~Q('exists', field='installations__closed'),
        Q('range', installations__closed={
          "gte": end
        })
      ])
    ]))

  return s[:0].execute()

def df(country, start, end, f=None, interval='month'):
  g = [] if f is None else deepcopy(f)

  BUCKETS = date_range(start, end, freq='MS')
  N = len(BUCKETS)

  df = DataFrame(columns=['clientes', 'kingos'])
  df.index.name = 'date'

  for i in range(N-1):
    cohort = BUCKETS[i].date().isoformat()
    response = search(country, BUCKETS[i], BUCKETS[i+1], f)
    # print(BUCKETS[i], BUCKETS[i+1], response.hits.total, response.aggs.installations.doc_count, response.aggs.installations.cohort.doc_count)
    df.loc[cohort] = [
      response.hits.total,
      response.aggs.installations.cohort.doc_count
    ]

  return df




