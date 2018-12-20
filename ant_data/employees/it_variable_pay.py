import datetime as dt
from dateutil.relativedelta import relativedelta
from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic
from ant_data.employees import hierarchy, hard_paid_days
from ant_data.static import CODES





def df(agent, start, end):
  communities = hierarchy.communities(agent)
  installs = hierarchy.installs(agent, start, end)

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

  end_dt = dt.datetime.strptime(end[:10], '%Y-%m-%d') # FIXME: local time
  month1_dt = end_dt - relativedelta(months=1)
  month2_dt = end_dt - relativedelta(months=2)
  month3_dt = end_dt - relativedelta(months=3)

  hard_days1 = hard_paid_days.df(client_ids, month1_dt, end_dt)
  hard_days2 = hard_paid_days.df(client_ids, month2_dt, month1_dt)
  hard_days3 = hard_paid_days.df(client_ids, month3_dt, month2_dt)

  obj = {}
  for client in clients:
    client_id = client['person_id']
    opened_dt = dt.datetime.strptime(client['opened'][:10], '%Y-%m-%d')
    model = client['installations'][0]['model']

    if opened_dt >= month3_dt and opened_dt < end_dt:
      if opened_dt < month2_dt:
        month = 'Mes 3'
        if hard_days3.loc[client_id]['active'] >= 15:
          cat = 'Activo >= 15'
        elif hard_days3.loc[client_id]['active'] >= 7:
          cat = 'Activo >= 7'
        else:
          cat = 'Inactivo'
      elif opened_dt < month1_dt:
        month = 'Mes 2'
        if hard_days2.loc[client_id]['active'] >= 15:
          cat = 'Activo >= 15'
        elif hard_days2.loc[client_id]['active'] >= 7:
          cat = 'Activo >= 7'
        else:
          cat = 'Inactivo'
      elif opened_dt < end_dt:
        month = 'Mes 1'
        if hard_days1.loc[client_id]['active'] >= 15:
          cat = 'Activo >= 15'
        elif hard_days1.loc[client_id]['active'] >= 7:
          cat = 'Activo >= 7'
        else:
          cat = 'Inactivo'

      obj[cat] = obj.get(cat, {})
      obj[cat][month] = obj[cat].get(month, {})
      obj[cat][month][model] = obj[cat][month].get(model, 0)
      obj[cat][month][model] += 1


  df = DataFrame.from_dict({(i, j): obj[i][j]
                          for i in obj.keys()
                          for j in obj[i].keys()},
                          orient='index')

  if df.empty:
    return df

  df = df.fillna(0)
  df.index = df.index.set_names('Activo', level=0)
  df.index = df.index.set_names('Mes', level=1)
  df = df.reset_index()

  def payment(row):
    active_factor = {
      'Activo >= 15': 1,
      'Activo >= 7': 1,
      'Inactivo': 0
    }
    month_factor = {
      'Mes 1': 1,
      'Mes 2': 1,
      'Mes 3': 1
    }
    model_factor = {
      'Kingo Básico': 5,
      'Kingo Luz': 5,
      'Kingo 15': 10,
      'Kingo TV': 30
    }

    f = active_factor.get(row['Activo'], 0) * month_factor.get(row['Mes'], 0)
    s = 0
    for x in model_factor:
      s += model_factor[x] * row.get(x, 0)
    return f * s

  df['Total'] = df[list(df)[2:]].sum(axis=1)
  df['Pago Instaladores'] = df.apply(payment, axis=1)
  df.loc['Total'] = df[list(df)[2:]].sum()
  # df = df[['Mes','Modelo','Activo','Total','Pago Instaladores']]
  df = df.fillna('')
  df[list(df)[2:]] = df[list(df)[2:]].astype('int64')

  return df