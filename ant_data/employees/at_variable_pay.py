import datetime as dt
from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic
from ant_data.hierarchy import hierarchy
from ant_data.static import CODES


def hard_client_days(agent, start, end):
  s = Search(using=elastic, index='people') \
    .query('term', doctype='client') \
    .query('term', community__employees__at__agent_id=agent) \
    .query('bool', should=[
      Q('term', open=True),
      Q('range', closed={ 'gte': end })
    ])

  #TODO: use partitions to get all the results
  # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html#_filtering_values_with_partitions
  s.aggs.bucket('clients', 'terms', field='person_id', size=10000) \
    .bucket('stats', 'children', type='stat') \
    .bucket('date_range', 'filter', filter=Q('range', date={ 'gte': start, 'lt': end })) \
    .bucket('active', 'terms', field='active')

  response = s[:0].execute()

  obj = {}
  for client in response.aggs.clients.buckets:
    obj[client.key] = {}
    for active in client.stats.date_range.active.buckets:
      obj[client.key][active.key] = active.doc_count

  df = DataFrame.from_dict(obj).T

  if df.empty:
    return df

  df.index.name = 'client_id'
  df = df.rename(columns={ 0: 'inactive', 1: 'active' })
  df['total'] = df.sum(axis=1)
  df = df.fillna(0).astype('int64')

  return df


def df(agent, start, end):
  communities = hierarchy.communities(agent)
  clients = hierarchy.clients(agent, end)
  codes = hierarchy.codes(agent, start, end)

  hard_days = hard_client_days(agent, start, end)

  code_map = {}
  for code in codes:
    client_id = code['to']['person_id']
    code_map[client_id] = code_map.get(client_id, { 'shopkeeper': 0, 'agent': 0 })
    if 'person_type' in code['from'] and code['from']['person_type'] == 'client':
      code_map[client_id]['shopkeeper'] += CODES.CODE_PLANS[code['plan']]
    else:
      code_map[client_id]['agent'] += CODES.CODE_PLANS[code['plan']]


  new_old = dt.datetime.strptime(start[:10], '%Y-%m-%d') - dt.timedelta(days=105)

  keys = ['Activo Compra Tendero >= 15', 'Activo Compra Tendero >= 7', 'Activo Nuevo >= 15', 'Activo Nuevo >= 10', 'Activo >= 15', 'Activo >= 7', 'Activo < 7', 'Inactivo']
  obj = {}
  for key in keys:
    obj[key] = 0

  for client in clients:
    client_id = client['person_id']
    opened = dt.datetime.strptime(client['opened'][:10], '%Y-%m-%d')
    days = hard_days.loc[client_id]['active']

    if opened < new_old:
      if code_map.get(client_id, { 'shopkeeper': 0 })['shopkeeper'] >= 15:
        cat = 'Activo Compra Tendero >= 15'
      elif code_map.get(client_id, { 'shopkeeper': 0 })['shopkeeper'] >= 7:
        cat = 'Activo Compra Tendero >= 7'
      elif days >= 15:
        cat = 'Activo >= 15'
      elif days >= 7:
        cat = 'Activo >= 7'
      elif days > 0:
        cat = 'Activo < 7'
      else:
        cat = 'Inactivo'
    else:
      if days >= 15:
        cat = 'Activo Nuevo >= 15'
      elif days >= 10:
        cat = 'Activo Nuevo >= 10'
      elif days > 0:
        cat = 'Activo < 7'
      else:
        cat = 'Inactivo'

    obj[cat] += 1

  df = DataFrame([obj[key] for key in keys ], index=keys, columns=['Parque'])
  df['Activo Mes Anterior'] = df['Parque']
  df.at['Inactivo', 'Activo Mes Anterior'] = 0
  df.loc['Total'] = df.sum()
  df['% Activo Mes Anterior'] = 100*round(df.at['Total', 'Activo Mes Anterior'] / df.at['Total', 'Parque'],3)
  factors = [ 8, 4, 8, 4, 4, 2, 0, 0 ]
  df['Pago x Actividad'] = df['% Activo Mes Anterior'] * Series(factors,index=keys)
  df.at['Total', 'Pago x Actividad'] = df['Pago x Actividad'].sum()
  df.index.name = 'Cuadrantes Mes Anterior'
  df = df.reset_index()

  return df