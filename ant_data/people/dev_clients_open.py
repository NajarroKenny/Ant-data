import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, date_range

from ant_data import elastic, ROOT_DIR

CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')

START = '2016-01-01'
END = '2019-01-01'
BUCKETS = date_range(START, END, freq='MS')

def df(country):
  df = DataFrame(index=BUCKETS[:-1], columns=['open'])

  for i in range(len(BUCKETS)-1):
    s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
      .query('term', country=country) \
      .query('term', doctype='client') \
      .query('range', kingo_opened={'lt': BUCKETS[i+1]}) \
      .query('bool', should=[
        ~Q('exists', field='kingo_closed'),
        Q('range', kingo_closed={'gte': BUCKETS[i+1]})
      ]) 
    
    df.loc[BUCKETS[i]] = s.execute().hits.total

  return df