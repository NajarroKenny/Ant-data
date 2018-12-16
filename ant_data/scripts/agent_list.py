"""
Agent list
============================
Contains functions to fetch Kingo's POS sync log from Postgres SQL and save it
to ant_data/static/AGENTS.py

- Create date:  2018-12-07
- Update date:  2018-12-07
- Version:      1.0

Notes:
============================      
- v1.0: Initial version
"""
import sys

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Timestamp

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


FNAME = sys.path[-1]+'/static/AGENT_LIST.py'
FHEADER = 'AGENT_LIST = [ \n'
FFOOTER = ']'


def search(f=None):
  s = Search(using=elastic, index='tasks') \
        .query('term', doctype='task')

  if f is not None:
      s = s.query('bool', filter=f)

  s.aggs.bucket('agents', 'terms', field='agent_id', size=10000, min_doc_count=1) 

  return s[:0].execute()

def df(f=None):
  response = search(f=f)

  obj = {}

  for agent in response.aggs.agents:
    obj[agent.key] = agent.doc_count

  df = DataFrame.from_dict(obj, orient='index', columns=['tasks']).sort_index()
  df.index.name = 'agent_id'
    
  return df


def agent_list(f=None):
  print('Starting to fetch agent list')
  agent_list = df(f=f)

  print('Writing agent list to file')
  with open(FNAME, 'w') as fh:
    fh.write(FHEADER)
    for agent in agent_list.index.tolist():
      fh.write(agent+'\n')
    fh.write(FFOOTER)
  print(f'Wrote agent list to file {FNAME}')

if __name__=='__main__':
  agent_list()

