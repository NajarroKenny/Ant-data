import configparser
from copy import deepcopy

from elasticsearch_dsl import Search, Q
import numpy as np
from pandas import DataFrame, date_range

from ant_data import elastic, ROOT_DIR
from ant_data.people import people_open
from ant_data.stats import dev_active_systems__date, dev_cohort_installs
from ant_data.codes import codes_to_kingos


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')

def df(country, start, end, f=None, paid=False, interval='month', model=None, version=None, model_version=None):
  g = [] if f is None else deepcopy(f)

  BUCKETS = date_range(start, end, freq='MS')
  N = len(BUCKETS)

  df = DataFrame(columns=['cohort', 'weighted', 'active_systems'])
  df.index.name = 'date'

  for i in range(N-1):
    cohort = BUCKETS[i].date().isoformat()
    cohort_filter = [Q('range', kingo_opened = {'gte': BUCKETS[i], 'lt': BUCKETS[i+1]})]

    open_systems = people_open.df_weighted(country, start=cohort, end=end, f=g+cohort_filter, interval=interval, model=model, version=version, model_version=model_version)

    active_systems = dev_active_systems__date.df(country, start=cohort, end=end, f=g+cohort_filter, interval=interval, model=model, version=version, model_version=model_version)

    # c2k = codes_to_kingos.df(country, start=start, end=end, f=g, interval=interval)

    df_cohort = open_systems.merge(active_systems, on='date', how='inner')

    df_cohort['cohort'] = cohort
    df_cohort = df_cohort[['cohort', 'weighted', 'active_systems']]
    df = df.append(df_cohort)

  ci = dev_cohort_installs.df('Guatemala', start, end)
  ci.index.name = 'cohort'
  ci = ci.reset_index()
  ci = ci[['cohort', 'kingos']]
  ci = ci.rename(columns={ 'kingos': 'installed' })

  df = df.reset_index()
  df = df.merge(ci, on='cohort', how='inner')
  df = df.set_index('date')

  df['% active'] = df['active_systems'] / df['installed']
  df['% weighted'] = df['weighted'] / df['installed']
  df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))

  df = df[['cohort', 'installed', 'weighted', '% weighted', 'active_systems', '% active']]
  df = df.rename(columns={
      'cohort': 'cohorte',
      'installed': 'instalados',
      'weighted': 'ponderados',
      '% weighted': '% ponderados',
      'active_systems': 'activos',
      '% active': '% activos'
    })

  return df




