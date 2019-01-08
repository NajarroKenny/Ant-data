"""
FINANCE
==========================
Static file with FINANCE related quantities

- Create date:  2018-11-??
- Update date:  2019-01-07
- Version:      1.1

Notes:
==========================
- v1.0: Initial version
- v1.1: Read FX from Elasticsearch instead of CSV
"""
import configparser

from elasticsearch_dsl import Search
from pandas import DataFrame

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')

IVA = {
  'Guatemala': 0.12,
  'Colombia': 0.17
}

CURRENCY = {
  'Guatemala': 'gtq',
  'Colombia': 'cop'
}

def fx(country=None, start=None, end=None, series=False):
  """Reads the exchange rate indexed in Elasticsearch and returns a DataFrame
  with the requested country and date range information (if passed).

  Args:
    country (str, optional): Guatemala or Colombia. Defaults to None, in which
      case it retrieves the exchange rate information for both countries.
    start (str, optional): ISO8601 date interval start. Defaults to None, in
      in which case the rates are fetched from 2013-01-01.
    end (str, optional): IOS8601 date interval end. Defaults to None, in which
      case the rates are fetched until the current local date.
    series (bool, optional): Flag to determine whether to return a Series or 
      DataFrame. Defaults to False, in which case a DataFrame is returned.
      
  Returns:
    DataFrame: Pandas DataFrame with exchange information, index = and columns = []
  """
  s = Search(using=elastic, index=CONFIG['ES']['FX'])

  if start is not None:
    s = s.query('range', date={'gte': start})

  if end is not None:
    s = s.query('range', date={'lt': end})

  rows = [x.to_dict() for x in s.scan()]
  fx = DataFrame(rows)
  fx = fx.set_index('date')
  fx = fx.reindex(fx.index.astype('datetime64')).sort_index()

  if country is not None:
    if series:
      fx = fx[CURRENCY.get(country)]
    else:
      fx = DataFrame(fx[CURRENCY.get(country)])

  return fx