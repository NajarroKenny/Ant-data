import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, date_range

from ant_data import elastic, ROOT_DIR

CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def df(country, start, end):
  BUCKETS = date_range(start, end, freq='MS')
  df = DataFrame(index=BUCKETS[:-1], columns=['end'])
  df.index.name = 'date'

  for i in range(len(BUCKETS)-1):
    # print(i, BUCKETS[i], BUCKETS[i+1])
    s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
      .query('term', country=country) \
      .query('term', doctype='client') \
      .query('range', opened={'lt': BUCKETS[i+1]}) \
      .query('bool', should=[
        ~Q('exists', field='closed'),
        Q('range', closed={'gte': BUCKETS[i+1]})
      ])

    df.loc[BUCKETS[i]] = s.execute().hits.total

  return df