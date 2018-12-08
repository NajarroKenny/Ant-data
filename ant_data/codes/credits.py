"""
Credits
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits

- Create date:  2018-11-30
- Update date:  2018-12-07
- Version:      1.1

Notes:
==========================        
- v1.0: Initial version
- v1.1: Updated with standard based on v1.1 of systems_opened
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ..static.FINANCE import IVA


def search(country, f=None, interval='month'):
  s = Search(using=elastic, index='codes') \
    .query('term', doctype='credit') \
    .query('bool', filter=Q('term', country=country))

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'dates', 'date_histogram', field='datetime', interval=interval,
    min_doc_count=0
  ) \
  .bucket('sales', 'terms', field='sale') \
  .metric('value', 'sum', field='value', missing=0) \
  .metric('commission', 'sum', field='commission', missing=0)

  return s[:0].execute()


def df(country, f=None, interval='month'):
  response = search(country, f=f, interval=interval)

  dates = [x.key_as_string for x in response.aggs.dates.buckets]
  obj = {x: { 'paid': 0, 'free': 0, 'commission':0 } for x in dates}

  for date in response.aggs.dates.buckets:
    free = 0
    paid = 0
    commission = 0

    for sale in date.sales.buckets:
      if sale.key_as_string == 'true':
        paid += sale.value.value
      elif sale.key_as_string == 'false':
        free += sale.value.value
      commission += sale.commission.value

    obj[date.key_as_string] = {
      'paid': paid,
      'free': free,
      'commission': commission
    }

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return DataFrame(columns=['free', 'paid', 'iva', 'commission', 'total'])  

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()

  df['paid'] = df['paid'] - df['commission']
  df['iva'] = IVA[country] * df['paid']
  df['paid'] = (1-IVA[country]) * df['paid']
  df['total'] = df.sum(axis=1)
  df = df.fillna(0).astype('double')
  df = df[['free', 'paid', 'iva', 'commission', 'total']]

  return df
