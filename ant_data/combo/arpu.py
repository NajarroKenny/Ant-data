import numpy as np
from pandas import DataFrame, MultiIndex

from ant_data.shared.helpers import join_df
from ant_data.stats import active__date as ad
from ant_data.stats import dev_active_systems__date as asd
from ant_data.people import people as p, dev_clients_open_from_installs as dev_clients_open
from ant_data.installs import installs as i
from ant_data.codes import codes_to_kingos, codes as c

def df(country, start=None, end=None, f=None, interval='month'):
  col1 = dev_clients_open.df(country, start, end)
  col2 = p.people_open(country, method='weighted', start=start, end=end, f=f, interval=interval)
  col3 = asd.df(country, start=start, end=end, f=f, interval=interval)
  col4 = i.kingos_open(country, start=start, end=end, f=f, interval=interval)['total']
  col5 = c.df(country, doctype='code', start=start, end=end, f=f, interval=interval)['paid']
  col6 = codes_to_kingos.df(country, start=start, end=end, f=f, interval=interval)
  col6['buying_kingos'] = col6['parents.paid'] + col6['orphan.paid']
  col6 = col6[['buying_kingos']]

  df = join_df('date', 'outer', col1, col2, col3, col4, col5, col6)
  df = df.sort_index().fillna(0)
  df = df.rename(columns={
    'end': 'clientes',
    'total': 'kingos',
    'weighted': 'kingos (ponderado)',
    'active_systems': 'kingos activos',
    'buying_kingos': 'kingos pagados',
    'paid': 'ingresos de clientes'
  })
  # df = df.rename(columns={x: x.lower() for x in col5.columns})
  df['% activos'] = df['kingos activos'] / df['kingos']
  df['% pagados'] = df['kingos pagados'] / df['kingos']
  df['arpu'] = df['ingresos de clientes'] / df['kingos (ponderado)']
  df['arpu activo'] = df['ingresos de clientes'] / df['kingos activos']
  df['arpu pagado'] = df['ingresos de clientes'] / df['kingos pagados']

  df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))

  df = df.astype({
    'clientes': 'int64',
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
    'clientes',
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


  # })

  return df