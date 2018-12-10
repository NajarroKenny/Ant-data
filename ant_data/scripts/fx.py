import requests

import bs4
from bs4 import BeautifulSoup
import pandas as pd
import re

FNAME = 'fx.csv'

FNAME = 'fx.csv'
START_DATE = '2013-01-01'
END_DATE = pd.Timestamp.utcnow()

gtq_url = (
  'http://www.banguat.gob.gt/cambio/historico.asp?kmoneda=02&ktipo=5&kmes=01&kdia=1&kanio=2013&kmes1={}&kdia1={}&kanio1={}&kcsv=OFF&submit1=Consultation'
  .format(END_DATE.date().month, END_DATE.date().day, END_DATE.date().year)
)
gtq_req = requests.get(gtq_url).text
gtq_soup = BeautifulSoup(gtq_req, 'html.parser')

gtq_data = dict()

for row in gtq_soup.find_all('tr'):
    td = [y.text.strip() for y in row if isinstance(y, bs4.element.Tag)]
    if len(td) == 4 and (re.search('\d{1,2}/\d{1,2}/\d{4}', td[0]) is not None):
        gtq_data[td[0]] = pd.to_numeric(td[3])
    else:
        continue

gtq = pd.Series(gtq_data)
gtq.index.name = 'date'
gtq = pd.DataFrame(gtq.values, index=pd.to_datetime(gtq.index, dayfirst=True)).rename(columns={0: 'gtq'})

cop_url = 'https://www.superfinanciera.gov.co/descargas?com=institucional&name=pubFile1010997&downloadname=historia.csv'
cop = pd.read_csv(cop_url, index_col='Fecha', usecols=['Fecha', 'TCRM'])
cop = cop.reindex(pd.to_datetime(cop.index))
cop = cop.apply(lambda x: x.str.replace(',',''))
cop = cop.loc[START_DATE: END_DATE].rename(columns={'TCRM': 'cop'}).astype('float64')

fx = gtq.join(cop, on='date', how='inner')

fx.to_csv(FNAME, header=True)
