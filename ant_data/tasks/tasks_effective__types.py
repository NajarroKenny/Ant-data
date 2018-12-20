"""
Tasks effective types
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task effectiveness by types.

- Create date:  2018-12-19
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST
from ant_data.static.TASK_TYPES import SALE_VALUES
from ant_data.tasks import tasks_workflow_list__types as twlt


WORKFLOW_LIST=[
  'active-code', 'install', 'pickup', 'register', 'swap', 'visit-install',
  'sale'
  ]

def search_task_sale_value(country, start=None, end=None, f=None):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  g = [] if f is None else f[:]

  if start is not None:
    g.append(Q('range', due={ 'gte': start }))
  
  if end is not None:
    g.append(Q('range', due={ 'lt': end }))

  g += [Q('term', planned=True)]

  s = Search(using=elastic, index='tasks') \
    .query('term', country=country) \
    .query('term', doctype='history') \
    .query('term', workflow=WORKFLOW_LIST[-1]) \
    .query('has_parent', parent_type='task', query=Q('bool', filter=g))

  return s.scan()

def search_gestion_tasks(country, start=None, end=None, f=None):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  s = Search(using=elastic, index='tasks') \
    .query('term', country=country) \
    .query('term', doctype='task') \
    .query('term', planned=True) \
    .query('has_child', type='history', query=Q('term', workflow=WORKFLOW_LIST[-1]))
    
  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  return s.scan()

def df_effective_sale(country, start=None, end=None, f=None, all=False):
  def is_effective(row):
    if row[1].startswith('gestion 3') and row[0] >= 14:
      return True
    elif row[1].startswith('gestion') and row[0] >= 7:
      return True
    elif ~row[1].startswith('gestion') and row[0] > 0:
      return True
    else:
      return False
  
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  hits = search_task_sale_value(country, start, end, f)
  
  obj = {}
   
  for hit in hits:  
    if hit.type in SALE_VALUES:
      task_id = hit.task_id
      amount = SALE_VALUES.get(hit.type, 0)
      obj[task_id] = obj.get(task_id, 0)  + amount
  
  if obj == {}:
    return DataFrame()
  
  df_sv = DataFrame.from_dict(obj, orient='index')
  df_sv.index.name = 'task_id'

  hits = search_gestion_tasks(country, start, end, f)

  obj = { hit.task_id: hit.remarks for hit in hits }

  if obj == {}:
    return DataFrame()

  df_g = DataFrame.from_dict(obj, orient='index')
  df_g.index.name = 'task_id'
  
  df = df_sv.merge(df_g, on='task_id', how='inner')
  df['effective'] = df.apply(is_effective, axis=1)
  df = df.drop('0_x', axis=1).groupby('0_y').sum()
  
  obj = {}

  for remark in df.index.tolist():
    if (
      remark.startswith('gestion') or 
      remark.startswith('gestion 2') or
      remark.startswith('gestion 3')
    ):
      tipo = 'Gestión'
    elif remark.startswith('ticket'):
      tipo = 'Ticket'
    elif remark.startswith('verificacion'):
      tipo = 'Verificación'
    elif remark.startswith('tendero venta'):
      tipo = 'Tendero Venta'
    elif remark.startswith('tendero sync'):
      tipo = 'Tendero Sync'
    elif remark.startswith('kingo basico'):
      tipo = 'Swap K7 o Venta >= 1 semana'
    elif remark.startswith('kingo tv'):
      tipo = 'Swap a Kingo TV'
    elif (
      remark.startswith('tecnica') or
      remark.startswith('promocion precio') or
      remark.startswith('corregir') or
      remark.startswith('cliente activo') or
      remark.startswith('asignacion especial tecnica')
    ):
      tipo = 'Técnica'
    # elif remark.startswith('instalacion k7'): FIXME:
    #     tipo = 'Instalación Kingo Básico'
    # elif remark.startswith('instalacion k15'):
    #     tipo = 'Instalación Kingo Luz'
    # elif remark.startswith('instalacion ktv'):
    #     tipo = 'Instalación Kingo TV'
    # elif remark.startswith('instalacion k100+'):
    #     tipo = 'Instalación Kingo Hogar'
    # elif (
    #     remark.startswith('preventa kingo tv') or
    #     remark.startswith('preventa: k15 tv') or
    #     remark.startswith('preventa: kingo tv')
    # ):
    #     tipo = 'Preventa Kingo TV'
    # elif remark.startswith('preventa kingo hogar'):
    #     tipo = 'Preventa Kingo Hogar'
    else:
      tipo = 'Sin Tipo'
    obj[tipo] = obj.get(tipo, 0) + df.at[remark, 'effective']
  
  df = DataFrame.from_dict(obj, orient='index', columns=['Tareas Efectivas'])
  df = df.astype('int64')

  if not all and 'Sin Tipo' in df.index:
        df = df.drop('Sin Tipo')
  
  df.loc['Total'] = df.sum()
  df.index.name = 'Tipo de Tarea'
  
  return df


def df(country, start=None, end=None, f=None, all=False):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')
  
  df = twlt.df(country, WORKFLOW_LIST[:-1], start, end, f, all)
  
  if df.empty:
    return df
  
  df = df.rename(columns={'Tareas': 'Tareas Efectivas'})

  df_sale = df_effective_sale(country, start, end, f, all)

  if df_sale.empty:
    return df

  for element in df.index:
    df.loc[element] += df_sale['Tareas Efectivas'].get(element, 0)

  return df
