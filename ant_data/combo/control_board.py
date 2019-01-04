import numpy as np
from pandas import DataFrame, MultiIndex

from ant_data.shared.helpers import join_df
from ant_data.stats import active__date as ad
from ant_data.stats import dev_active_systems__date as asd
from ant_data.people import people as p, dev_clients_open_from_installs as dev_clients_open
from ant_data.installs import installs as i
from ant_data.codes import codes as c

def df(country, start=None, end=None, f=None, interval='month'):
  col1 = dev_clients_open.df(country, start, end)
  col2 = p.people_open(country, method='weighted', start=start, end=end, f=f, interval=interval)
  col3 = asd.df(country, start=start, end=end, paid=False, f=f, interval=interval)
  col4 = asd.df(country, start=start, end=end, paid=True, f=f, interval=interval)
  col5 = i.kingos_open(country, start=start, end=end, f=f, interval=interval)['total']
  col6 = c.df(country, doctype='code', start=start, end=end, f=f, interval=interval)['paid']

  df = join_df('date', 'outer', col1, col2, col3, col4, col5, col6)
  df = df.sort_index().fillna(0)
  df = df.rename(columns={
    'end': 'clientes',
    'weighted': 'kingos (ponderado)',
    'active_systems_x': 'kingos activos',
    'active_systems_y': 'kingos activos pagado',
    'total': 'kingos',
    'paid': 'ingresos de clientes',
    'no_model': 'sin modelo'
  })
  # df = df.rename(columns={x: x.lower() for x in col5.columns})
  df['% activos pagado'] = df['kingos activos pagado'] / df['kingos']
  df['% activos'] = df['kingos activos'] / df['kingos']
  df['arpu'] = df['ingresos de clientes'] / df['kingos (ponderado)']

  df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))

  df = df.astype({
    'clientes': 'int64',
    'kingos': 'int64',
    'kingos (ponderado)': 'int64',
    'kingos activos': 'int64',
    'kingos activos pagado': 'int64'
  })

  df[['% activos pagado', '% activos']] = df[['% activos pagado', '% activos']]*100
  df['arpu activo'] = df['ingresos de clientes'] / df['kingos activos']

  df = df.round({
    'ingresos de cliente': 2,
    '% activos pagado': 1,
    '% activos': 1,
    'arpu': 2,
    'arpu activo': 2
  })

  df = df[[
    'clientes',
    'kingos',
    'kingos (ponderado)',
    'kingos activos',
    '% activos',
    'kingos activos pagado',
    '% activos pagado',
    'ingresos de clientes',
    'arpu',
    'arpu activo'
  ]]


  # })

  return df