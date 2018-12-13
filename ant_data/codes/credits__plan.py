"""
Credits by Plan
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on credits by plan

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
  ).bucket('plans', 'terms', field='plan') \
  .bucket('sales', 'terms', field='sale', min_doc_count=0) \
  .metric('value', 'sum', field='value', missing=0) \
  .metric('commission', 'sum', field='commission', missing=0)

  return s[:0].execute()


def df(
  country, f=None, interval='month', paid=True, free=True, iva=True,
  commission=True
):
  response = search(country, f=f, interval=interval)

  obj = {}

  for date in response.aggs.dates.buckets:
    obj[date.key_as_string] = {}

    for plan in date.plans.buckets:
      f = 0
      p = 0
      i = 0
      c = 0

      for sale in plan.sales.buckets:
        if sale.key_as_string == 'true':
          p += sale.value.value
        elif sale.key_as_string == 'false':
          f += sale.value.value
        c += sale.commission.value

      # IVA
      i = IVA[country] * p
      p = (1 - IVA[country]) * p

      value = 0
      value += p if paid else 0
      value += i if iva else 0
      value += f if free else 0
      value += c if commission else 0

      obj[date.key_as_string][plan.key] = value

  df = DataFrame.from_dict(obj, orient='index')

  if df.empty:
    return DataFrame(columns=[''])

  df.index.name = 'date'
  df = df.reindex(df.index.astype('datetime64')).sort_index()
  df = df.fillna(0).astype('double')
  df['total'] = df.sum(axis=1)

  return df
