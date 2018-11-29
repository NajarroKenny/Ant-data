import requests

import bs4
from bs4 import BeautifulSoup
import pandas as pd
import re

FNAME = 'fx.csv'

gtq_url = 'http://www.banguat.gob.gt/cambio/historico.asp?kmoneda=02&ktipo=5&kmes=01&kdia=1&kanio=2012&kmes1=11&kdia1=28&kanio1=2018&kcsv=OFF&submit1=Consultation'

gtq_req = requests.get(gtq_url).text

gtq_soup = BeautifulSoup(gtq_req, 'html.parser')

data = dict()

for row in gtq_soup.find_all('tr'):
    td = [y.text.strip() for y in row if isinstance(y, bs4.element.Tag)]
    if len(td) == 4 and (re.search('\d{1,2}/\d{1,2}/\d{4}', td[0]) is not None):
        data[td[0]] = pd.to_numeric(td[3])
    else:
        continue

fx = pd.Series(data)
fx = fx.reindex(pd.to_datetime(fx.index)).sort_index()
fx.name = 'gtq'
fx.index.name = 'date'
fx.to_csv(FNAME, header=True)
