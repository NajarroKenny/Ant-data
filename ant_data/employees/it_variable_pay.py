import datetime as dt
from dateutil.relativedelta import relativedelta
from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import pandas as pd
import numpy as np

from ant_data import elastic
from ant_data.shared import helpers
from ant_data.employees import hierarchy, hard_paid_days, hard_shopkeeper_days
from ant_data.static import CODES

def df(date, agent_id, hierarchy_id=None):
  month1 = helpers.shift_date_str(date, months=-1)
  month2 = helpers.shift_date_str(date, months=-2)
  month3 = helpers.shift_date_str(date, months=-3)

  installs = hierarchy.agent_installs(agent_id, month3, date)

  # client ids
  client_ids = []
  for install in installs:
    client_ids.append(install['person_id'])

  # client search
  s = Search(using=elastic, index='people') \
    .query('ids', type='_doc', values=client_ids)

  # clients
  clients = []
  for hit in s.scan():
    clients.append(hit.to_dict())

  shopkeeper_days = hard_shopkeeper_days.df(client_ids, month1, date)
  hard_days1 = hard_paid_days.df(client_ids, month1, date)
  hard_days2 = hard_paid_days.df(client_ids, month2, month1)
  hard_days3 = hard_paid_days.df(client_ids, month3, month2)

  obj = {}
  for client in clients:
    client_id = client['person_id']
    opened = client['opened']
    model = client['installations'][0]['model']
    days1 = hard_days1.loc[client_id]['active']
    days2 = hard_days2.loc[client_id]['active']
    days3 = hard_days3.loc[client_id]['active']

    if opened >= month3 and opened < date:
      if opened < month2:
        month = 'mes 3'
        if days3 >= 15:
          cat = 'activo >= 15'
        elif days3 >= 10:
          cat = 'activo >= 10'
        else:
          cat = 'inactivo'
      elif opened < month1:
        month = 'mes 2'
        if days2 >= 15:
          cat = 'activo >= 15'
        elif days2 >= 10:
          cat = 'activo >= 10'
        else:
          cat = 'inactivo'
      elif opened < date:
        month = 'mes 1'
        if days1 >= 15:
          cat = 'activo >= 15'
        elif days1 >= 10:
          cat = 'activo >= 10'
        else:
          cat = 'inactivo'

      obj[cat] = obj.get(cat, {})
      obj[cat][month] = obj[cat].get(month, {})
      obj[cat][month][model] = obj[cat][month].get(model, 0)
      obj[cat][month][model] += 1


  df = DataFrame.from_dict({(i, j): obj[i][j]
                          for i in obj.keys()
                          for j in obj[i].keys()},
                          orient='index')

  if df.empty:
    return {
      'df': df,
      'park': 0
    }

  df = df.fillna(0)
  df.index = df.index.set_names('activo', level=0)
  df.index = df.index.set_names('mes', level=1)

  index = pd.MultiIndex.from_tuples([ ('activo >= 15', 'mes 1'), ('activo >= 10', 'mes 1'), ('inactivo', 'mes 1'), ('activo >= 15', 'mes 2'), ('activo >= 10', 'mes 2'), ('inactivo', 'mes 2'), ('activo >= 15', 'mes 3'), ('activo >= 10', 'mes 3'), ('inactivo', 'mes 3')], names=['activo', 'mes'])

  dff = DataFrame(index=index)
  dff = dff.merge(df, on=['activo', 'mes'], how='left')
  dff = dff.fillna(0)
  dff = dff.swaplevel(0, 1)
  df = dff.reset_index()

  shopkeeper_days = shopkeeper_days.sum()
  hard_days1 = hard_days1.sum()
  percent_active = hard_days1['active'] / hard_days1['total']
  percent_sk_active = shopkeeper_days['active'] / hard_days1['active'] if hard_days1['active'] != 0 else 0

  active_park_payment = 0
  if (percent_active >= 0.75):
    if (percent_sk_active < 0.75):
      active_park_payment = 1000
    if (percent_sk_active >= 0.75):
      active_park_payment = 1200
  if (percent_active >= 0.6):
    if (percent_sk_active < 0.75):
      active_park_payment = 600
    if (percent_sk_active >= 0.75):
      active_park_payment = 1000
  if (percent_active >= 0.5):
    if (percent_sk_active < 0.75):
      active_park_payment = 400
    if (percent_sk_active >= 0.75):
      active_park_payment = 800

  payments = { # FIXME: P1 get real formula
      'Kingo Básico': {
        'activo >= 15': {
          'mes 1': 5,
          'mes 2': 15,
          'mes 3': 20
        },
        'activo >= 10': {
          'mes 1': 5,
          'mes 2': 5,
          'mes 3': 5
        },
        'inactivo': {
          'mes 1': 0,
          'mes 2': 0,
          'mes 3': 0
        }
      },
      'Kingo - Luz': {
        'activo >= 15': {
          'mes 1': 15,
          'mes 2': 25,
          'mes 3': 30
        },
        'activo >= 10': {
          'mes 1': 5,
          'mes 2': 5,
          'mes 3': 5
        },
        'inactivo': {
          'mes 1': 0,
          'mes 2': 0,
          'mes 3': 0
        }
      },
      'Kingo - Basico': {
        'activo >= 15': {
          'mes 1': 5,
          'mes 2': 15,
          'mes 3': 20
        },
        'activo >= 10': {
          'mes 1': 5,
          'mes 2': 5,
          'mes 3': 5
        },
        'inactivo': {
          'mes 1': 0,
          'mes 2': 0,
          'mes 3': 0
        }
      },
      'Kingo 15': {
        'activo >= 15': {
          'mes 1': 5,
          'mes 2': 15,
          'mes 3': 20
        },
        'activo >= 10': {
          'mes 1': 5,
          'mes 2': 5,
          'mes 3': 5
        },
        'inactivo': {
          'mes 1': 0,
          'mes 2': 0,
          'mes 3': 0
        }
      },
      'Kingo TV': {
        'activo >= 15': {
          'mes 1': 25,
          'mes 2': 30,
          'mes 3': 35
        },
        'activo >= 10': {
          'mes 1': 5,
          'mes 2': 5,
          'mes 3': 5
        },
        'inactivo': {
          'mes 1': 0,
          'mes 2': 0,
          'mes 3': 0
        }
      }
    }

  for x in list(df)[2:]:
    if x not in payments.keys():
      print(f'Model missing from payment object: {x}')

  def payment(row):
    payment = 0
    for x in payments.keys():
      if x in list(df):
        payment += row[x] * payments[x][row['activo']][row['mes']]

    return payment

  df['total'] = df[list(df)[2:]].sum(axis=1)
  df['pago'] = df.apply(payment, axis=1)
  df.loc['total'] = df[list(df)[2:]].sum()
  df = df.fillna('')
  df[list(df)[2:]] = df[list(df)[2:]].astype('int64')

  df2 = DataFrame([[percent_active, percent_sk_active, active_park_payment]], columns=['% activo','% tendero','pago'], index=[0])
  print(df2)

  return {
    'df1': df,
    'df2': df2
  }