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

def df(country, start, end, f=None, paid=False, interval='month'):
  g = [] if f is None else deepcopy(f)
  
  BUCKETS = date_range(start, end, freq='MS')
  N = len(BUCKETS)
  
  df = DataFrame(columns=['cohorte', 'kingos ponderados', 'kingos activos', '% kingos activos'])
  df.index.name = 'date' 

  for i in range(N-1):
    cohort = BUCKETS[i].date().isoformat()
    cohort_filter = [Q('range', kingo_opened = {'gte': BUCKETS[i], 'lt': BUCKETS[i+1]})] 
    open_systems = people_open.df(country, method='weighted', start=cohort, end=end, f=g+cohort_filter, interval=interval)
    active_systems = dev_active_systems__date.df(country, start=cohort, end=end, f=g+cohort_filter, paid=False, interval=interval)
    df_cohort = open_systems.merge(active_systems, on='date', how='inner')
    df_cohort = df_cohort.rename(columns={
      'weighted': 'kingos ponderados',
      'active_systems': 'kingos activos'
    })
    df_cohort['% kingos activos'] = df_cohort['kingos activos'] / df_cohort['kingos ponderados']
    df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))
    df_cohort['cohorte'] = cohort
    df_cohort = df_cohort[['cohorte', 'kingos ponderados', 'kingos activos', '% kingos activos']]
    df = df.append(df_cohort)

  return df

    


  