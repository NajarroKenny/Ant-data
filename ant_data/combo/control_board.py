from pandas import DataFrame

from ant_data.stats import active__date as ad
from ant_data.people import people as p
from ant_data.systems import systems as s
from ant_data.codes import codes as c

def df(country, start=None, end=None, f=None, interval='month'):

  col1 = DataFrame(p.clients_open(country, start=start, end=end, f=f, interval=interval))
  col2 = DataFrame(p.clients_open(country, method='weighted', start=start, end=end, f=f, interval=interval))
  col3 = ad.df(country, start=start, end=end, f=f, interval=interval)
  col4 = ad.df(country, start=start, end=end, paid=False, f=f, interval=interval)
  col5 = s.kingos_open(country, start=start, end=end, f=f, interval=interval)
  col6 = DataFrame(c.df(country, doctype='code', start=start, end=end, f=f, interval=interval)['paid'])

  df = col1.merge(col2, on='date', how='outer')
  df = df.merge(col3, on='date', how='outer')
  df = df.merge(col4, on='date', how='outer')
  df = df.merge(col5, on='date', how='outer')
  df = df.merge(col6, on='date', how='outer')

  df = df.sort_index().fillna(0)
  df = df.rename(columns={
    'end': 'clientes abiertos (fin del mes)',
    'weighted': 'kingos abiertos (ponderado)',
    'active_x': 'clientes activos pagado',
    'active_y': 'clientes activos',
    'total': 'kingos abiertos (fin del mes)',
    'paid': 'ingresos de cliente'
  })
  df['% clientes activos pagado'] = df['clientes activos pagado'] / df['clientes abiertos (fin del mes)']
  df['% clientes activos'] = df['clientes activos'] / df['clientes abiertos (fin del mes)']
  df['ARPU'] = df['ingresos de cliente'] / df['kingos abiertos (ponderado)']
  # df['ARPU activo'] = df['ingresos de cliente'] / df['kingos abiertos (ponderado)']

  # df = df[['abiertos',]]
  # df = df.astype({

  # })

  return df