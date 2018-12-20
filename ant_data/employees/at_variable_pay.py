from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic
from ant_data.employees import hierarchy, hard_paid_days
from ant_data.static import CODES
from ant_data.shared import helpers


def df(hierarchy_id, agent, start, end):
  # breakpoint()
  communities = hierarchy.communities(hierarchy_id, agent)
  clients = hierarchy.clients(communities, end)
  codes = hierarchy.codes(communities, start, end)
  client_ids = [ client['person_id'] for client in clients ]
  hard_days = hard_paid_days.df(client_ids, start, end)

  code_map = {}
  for code in codes:
    client_id = code['to']['person_id']
    code_map[client_id] = code_map.get(client_id, { 'shopkeeper': 0, 'agent': 0 })
    if 'person_type' in code['from'] and code['from']['person_type'] == 'client':
      code_map[client_id]['shopkeeper'] += CODES.CODE_PLANS[code['plan']]
    else:
      code_map[client_id]['agent'] += CODES.CODE_PLANS[code['plan']]


  new_old = helpers.shift_date_str(end, days=-105) # FIXME:check with Chino

  keys = ['Activo Compra Tendero >= 15', 'Activo Compra Tendero >= 7', 'Activo Nuevo >= 15', 'Activo Nuevo >= 10', 'Activo >= 15', 'Activo >= 7', 'Activo < 7', 'Inactivo']
  obj = {}
  for key in keys:
    obj[key] = 0

  for client in clients:
    client_id = client['person_id']
    opened = client['opened']
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

  if df.empty:
    return df

  df['Activo'] = df['Parque']
  df.at['Inactivo', 'Activo'] = 0
  df.at['Activo < 7', 'Activo'] = 0
  df.loc['Total'] = df.sum()
  df['% Activo'] = 100*round(df.at['Total', 'Activo'] / df.at['Total', 'Parque'],3)
  if df.at['Total', '% Activo'] > 85:
    factors = [ 12, 6, 12, 6, 8, 4, 0, 0 ]
  elif df.at['Total', '% Activo'] > 75:
    factors = [ 8, 4, 8, 4, 4, 2, 0, 0 ]
  elif df.at['Total', '% Activo'] > 50:
    factors = [ 4, 2, 4, 2, 2, 1, 0, 0 ]
  elif df.at['Total', '% Activo'] > 40:
    factors = [ 2, 1, 2, 1, 1, 0.5, 0, 0 ]
  else:
    factors = [ 0, 0, 0, 0, 0, 0, 0, 0 ]

  df['Pago'] = df['Activo'] * Series(factors,index=keys)
  df.at['Total', 'Pago'] = df['Pago'].sum()
  df.index.name = 'Cuadrantes'
  df = df.reset_index()

  return df