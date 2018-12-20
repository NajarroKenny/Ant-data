from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic
from ant_data.employees import hierarchy, hard_paid_days, hard_shopkeeper_days
from ant_data.static import CODES
from ant_data.shared import helpers


def df(start, end, communities=None, agent_id=None, hierarchy_id=None):
  # breakpoint()
  clients = hierarchy.client_docs(communities=communities, agent_id=agent_id, hierarchy_id=hierarchy_id, date=end)
  client_ids = [ client['person_id'] for client in clients ]
  shopkeeper_days = hard_shopkeeper_days.df(client_ids, start, end)
  paid_days = hard_paid_days.df(client_ids, start, end)

  new_old = helpers.shift_date_str(end, days=-105) # FIXME:check with Chino

  keys = ['activo compra tendero >= 15', 'activo compra tendero >= 7', 'activo nuevo >= 15', 'activo nuevo >= 10', 'activo >= 15', 'activo >= 7', 'activo < 7', 'inactivo']
  obj = {}
  for key in keys:
    obj[key] = 0

  for client in clients:
    client_id = client['person_id']
    opened = client['opened']
    days = paid_days.loc[client_id]['active']
    shopkeeper_days = paid_days.loc[client_id]['active']

    if opened < new_old:
      if shopkeeper_days >= 15: # FIXME:check with Chino
        cat = 'activo compra tendero >= 15'
      elif shopkeeper_days >= 7: # FIXME:check with Chino
        cat = 'activo compra tendero >= 7'
      elif days >= 15:
        cat = 'activo >= 15'
      elif days >= 7:
        cat = 'activo >= 7'
      elif days > 0:
        cat = 'activo < 7'
      else:
        cat = 'inactivo'
    else:
      if days >= 15:
        cat = 'activo nuevo >= 15'
      elif days >= 10:
        cat = 'activo nuevo >= 10'
      elif days > 0:
        cat = 'activo < 7'
      else:
        cat = 'inactivo'

    obj[cat] += 1

  df = DataFrame([obj[key] for key in keys ], index=keys, columns=['parque'])

  if df.empty:
    return df

  df['activo'] = df['parque']
  df.at['inactivo', 'activo'] = 0
  df.at['activo < 7', 'activo'] = 0
  df.loc['total'] = df.sum()
  df['% activo'] = df.at['total', 'activo'] / df.at['total', 'parque']
  if df.at['total', '% activo'] > 0.85:
    factors = [ 12, 6, 12, 6, 8, 4, 0, 0 ]
  elif df.at['total', '% activo'] > 0.75:
    factors = [ 8, 4, 8, 4, 4, 2, 0, 0 ]
  elif df.at['total', '% activo'] > 0.50:
    factors = [ 4, 2, 4, 2, 2, 1, 0, 0 ]
  elif df.at['total', '% activo'] > 0.40:
    factors = [ 2, 1, 2, 1, 1, 0.5, 0, 0 ]
  else:
    factors = [ 0, 0, 0, 0, 0, 0, 0, 0 ]

  df['pago'] = df['activo'] * Series(factors, index=keys)
  df.at['total', 'pago'] = df['pago'].sum()
  df.index.name = 'cuadrantes'
  df = df.reset_index()

  return df