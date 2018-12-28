"""
Credits
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits

- Create date:  2018-11-30
- Update date:  2018-12-26
- Version:      1.3

Notes:
==========================
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
- v1.2: Added free, paid, iva, commission filters
- v1.3: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ..static.FINANCE import IVA


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, doctype, start=None, end=None, f=None, interval='month'):
  s = Search(using=elastic, index=CONFIG['ES']['CODES']) \
    .query('term', doctype=doctype) \
    .query('bool', filter=Q('term', country=country))

  if start is not None:
    s = s.query('bool', filter=Q('range', datetime={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', datetime={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'dates', 'date_histogram', field='datetime', interval=interval,
    min_doc_count=1
  ) \
  .bucket('sales', 'terms', field='sale') \
  .metric('value', 'sum', field='value', missing=0) \
  .metric('commission', 'sum', field='commission', missing=0)

  return s[:0].execute()


def df(
  country, doctype, start=None, end=None, f=None, interval='month', paid=True, free=True, iva=True,
  commission=True
):
  response = search(country, doctype, start=start, end=end, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.dates.buckets]
  obj = {x: { 'paid': 0, 'free': 0, 'commission':0 } for x in dates}

  for date in response.aggs.dates.buckets:
    free_value = 0
    paid_value = 0
    commission_value = 0

    for sale in date.sales.buckets:
      if sale.key_as_string == 'true':
        paid_value += sale.value.value
      elif sale.key_as_string == 'false':
        free_value += sale.value.value
      commission_value += sale.commission.value

    obj[date.key_as_string] = {
      'paid': paid_value,
      'free': free_value,
      'commission': commission_value
    }

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    df = DataFrame(columns=['free', 'paid', 'iva', 'commission', 'total'])
    df.index.name = 'date'
    return df

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  df['paid'] = df['paid'] - df['commission']
  df['iva'] = IVA[country] * df['paid']
  df['paid'] = (1-IVA[country]) * df['paid']
  df['total'] = df.sum(axis=1)

  if not paid:
    df['paid'] = 0
  if not free:
    df['free'] = 0
  if not commission:
    df['commission'] = 0
  if not iva:
    df['iva'] = 0

  df = df.fillna(0).astype('double')
  df = df[['free', 'paid', 'iva', 'commission', 'total']]

  return df
