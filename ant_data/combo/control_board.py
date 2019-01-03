import numpy as np
from pandas import DataFrame, MultiIndex

from ant_data.shared.helpers import join_df
from ant_data.stats import active__date as ad
from ant_data.people import people as p
from ant_data.systems import systems as s
from ant_data.codes import codes as c

def df(country, start=None, end=None, f=None, interval='month'):

  col1 = p.clients_open(country, start=start, end=end, f=f, interval=interval)
  col2 = p.clients_open(country, method='weighted', start=start, end=end, f=f, interval=interval)
  col3 = ad.df(country, start=start, end=end, f=f, interval=interval)
  col4 = ad.df(country, start=start, end=end, paid=False, f=f, interval=interval)
  col5 = s.kingos_open(country, start=start, end=end, f=f, interval=interval)
  col6 = c.df(country, doctype='code', start=start, end=end, f=f, interval=interval)['paid']

  df = join_df('date', 'outer', col1, col2, col3, col4, col5, col6)
 
  df = df.sort_index().fillna(0)
  df = df.rename(columns={
    'end': 'clientes abiertos (fin del mes)',
    'weighted': 'kingos abiertos (ponderado)',
    'active_x': 'clientes activos pagado',
    'active_y': 'clientes activos',
    'total': 'kingos abiertos (fin del mes)',
    'paid': 'ingresos de clientes',
    'no_model': 'sin modelo'
  })
  df = df.rename(columns={x: x.lower() for x in col5.columns})
  df['% clientes activos pagado'] = df['clientes activos pagado'] / df['clientes abiertos (fin del mes)']
  df['% clientes activos'] = df['clientes activos'] / df['clientes abiertos (fin del mes)']
  df['ARPU'] = df['ingresos de clientes'] / df['kingos abiertos (ponderado)']

  df = df.replace((-np.inf, np.inf, np.nan), (0,0,0))
  df = df.astype({
    'clientes abiertos (fin del mes)': 'int64', 
    'kingos abiertos (ponderado)': 'int64',
    'clientes activos pagado': 'int64', 
    'clientes activos': 'int64', 
    'sin modelo': 'int64', 
    'kingo 100': 'int64',
    'kingo 10': 'int64',       
    'kingos abiertos (fin del mes)': 'int64',
    'kingo básico': 'int64', 
    'kingo tv': 'int64',
    'kingo luz': 'int64',
  })
  
  df[['% clientes activos pagado', '% clientes activos']] = df[['% clientes activos pagado', '% clientes activos']]*100
  df = df.round({
    'ingresos de cliente': 2, 
    '% clientes activos pagado': 1,
    '% clientes activos': 1, 
    'ARPU': 2
  })
  
  # df['ARPU activo'] = df['ingresos de cliente'] / df['kingos abiertos (ponderado)']

  df = df[[
    'clientes abiertos (fin del mes)', 
    'clientes activos pagado', 
    '% clientes activos pagado',
    'clientes activos',
    '% clientes activos',
    'kingo luz', 
    'kingo básico', 
    'kingo 10', 
    'kingo tv', 
    'kingo 100', 
    'sin modelo', 
    'kingos abiertos (fin del mes)',
    'kingos abiertos (ponderado)',
    'ingresos de clientes',
    'ARPU'
  ]]
  

  # })

  return df