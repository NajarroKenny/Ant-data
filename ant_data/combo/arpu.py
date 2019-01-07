from copy import deepcopy

from elasticsearch_dsl import Q
import numpy as np
from pandas import DataFrame, MultiIndex

from ant_data.shared.helpers import join_df
from ant_data.stats import active__date as ad
from ant_data.stats import dev_active_systems__date as asd
from ant_data.people import people_open, installs_open
from ant_data.codes import codes_to_kingos, codes as c

def df(country, start=None, end=None, f=None, interval='month', model=None, version=None, model_version=None):

  col1 = people_open.df_weighted(country, start=start, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version)
  col2 = asd.df(country, start=start, end=end, f=f, interval=interval, model=model, version=version, model_version=model_version)
  col3 = installs_open.df(country, start=start, end=end, f=f, model=model, version=version, model_version=model_version, system_type='kingo', interval=interval)['total']

  g = [] if f is None else deepcopy(f)
  if model is not None and model != []:
    model = model if isinstance(model, list) else [ model ]
    g.append(Q('terms', to__model=model))
  if version is not None and version != []:
    version = version if isinstance(version, list) else [ version ]
    g.append(Q('terms', to__version=version))
  if model_version is not None and model_version != []:
    model_version = model_version if isinstance(model_version, list) else [ model_version ]
    g.append(Q('terms', to__model_version=model_version))

  col4 = c.df(country, doctype='code', start=start, end=end, f=g, interval=interval)['paid']
  col5 = codes_to_kingos.df(country, start=start, end=end, f=g, interval=interval)
  col5['buying_kingos'] = col5['parents.paid'] + col5['orphan.paid']
  col5 = col5[['buying_kingos']]

  df = join_df('date', 'outer', col1, col2, col3, col4, col5)
  df = df.sort_index().fillna(0)
  df = df.rename(columns={
    'total': 'kingos',
    'weighted': 'kingos (ponderado)',
    'active_systems': 'kingos activos',
    'buying_kingos': 'kingos pagados',
    'paid': 'ingresos de clientes'
  })

  df['% activos'] = df['kingos activos'] / df['kingos']
  df['% pagados'] = df['kingos pagados'] / df['kingos']
  df['arpu'] = df['ingresos de clientes'] / df['kingos (ponderado)']
  df['arpu activo'] = df['ingresos de clientes'] / df['kingos activos']
  df['arpu pagado'] = df['ingresos de clientes'] / df['kingos pagados']

  df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))

  df = df.astype({
    'kingos': 'int64',
    'kingos (ponderado)': 'int64',
    'kingos activos': 'int64',
    'kingos pagados': 'int64',
  })

  df[['% pagados', '% activos']] = df[['% pagados', '% activos']]*100

  df = df.round({
    'ingresos de clientes': 2,
    '% activos': 1,
    '% pagados': 1,
    'arpu': 2,
    'arpu activo': 2,
    'arpu pagado': 2
  })

  df = df[[
    'kingos',
    'kingos (ponderado)',
    'kingos activos',
    '% activos',
    'kingos pagados',
    '% pagados',
    'ingresos de clientes',
    'arpu',
    'arpu activo',
    'arpu pagado'
  ]]

  return df