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
from ant_data.static.TASK_TYPES import SALE_VALUES
from ant_data.tasks import assigned_tasks


WORKFLOW_LIST=[
  'active-code', 'install', 'pickup', 'register', 'swap', 'visit-install'
]

def search_sale_values(start=None, end=None, f=None):
  g = [] if f is None else f[:]

  if start is not None:
    g.append(Q('range', due={ 'gte': start }))

  if end is not None:
    g.append(Q('range', due={ 'lt': end }))

  g += [Q('term', planned=True)]

  s = Search(using=elastic, index='tasks') \
    .query('term', doctype='history') \
    .query('term', workflow='sale') \
    .query('has_parent', parent_type='task', query=Q('bool', filter=g))

  return s.scan()

def search_sale_tasks(start=None, end=None, f=None):
  s = Search(using=elastic, index='tasks') \
    .query('term', doctype='task') \
    .query('term', planned=True) \
    .query('has_child', type='history', query=Q('term', workflow='sale'))

  if start is not None:
    s = s.query('bool', filter=Q('range', due={ 'gte': start }))
  if end is not None:
    s = s.query('bool', filter=Q('range', due={ 'lt': end }))
  if f is not None:
    s = s.query('bool', filter=f)

  return s.scan()

def df_effective_sale(start=None, end=None, f=None, all=False):
  """Determines if sales are effective

  Effective means sale is sufficient for a given task type.
  """

  def is_effective(row):
    if row[1].startswith('gestion 3') and row[0] >= 14:
      return True
    elif row[1].startswith('gestion') and row[0] >= 7:
      return True
    elif ~row[1].startswith('gestion') and row[0] > 0:
      return True
    else:
      return False

  hits = search_sale_values(start, end, f)

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

  hits = search_sale_tasks(start, end, f)

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
      tipo = 'gestión'
    elif remark.startswith('ticket'):
      tipo = 'ticket'
    elif remark.startswith('verificacion'):
      tipo = 'verificación'
    elif remark.startswith('tendero venta'):
      tipo = 'tendero Venta'
    elif remark.startswith('tendero sync'):
      tipo = 'tendero Sync'
    elif remark.startswith('kingo basico'):
      tipo = 'swap k7 o venta >= 1 semana'
    elif remark.startswith('kingo tv'):
      tipo = 'swap a kingo tv'
    elif (
      remark.startswith('tecnica') or
      remark.startswith('promocion precio') or
      remark.startswith('corregir') or
      remark.startswith('cliente activo') or
      remark.startswith('asignacion especial tecnica')
    ):
      tipo = 'técnica'
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
      tipo = 'sin tipo'
    obj[tipo] = obj.get(tipo, 0) + df.at[remark, 'effective']

  df = DataFrame.from_dict(obj, orient='index', columns=['efectivas'])
  df = df.astype('int64').sort_index()

  if not all and 'sin tipo' in df.index:
        df = df.drop('sin tipo')
  
  df.loc['total'] = df.sum()
  df.index.name = 'tipo de tarea'

  return df


def df(start=None, end=None, f=None, all=False):
  # Successful non-sale workflows
  df = assigned_tasks.df(workflow=WORKFLOW_LIST, start=start, end=end, f=f, all=all)
  if not df.empty:
    df = df.rename(columns={'asignadas': 'efectivas'})

  # Successful sales
  df_sale = df_effective_sale(start, end, f, all)

  if df_sale.empty:
    return df

  for element in df.index:
    df.loc[element] += df_sale['efectivas'].get(element, 0)

  return df
