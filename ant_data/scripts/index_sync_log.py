"""
Index sync Log
==========================
Contains functions to fetch Kingo's POS sync log from Postgres SQL and index it
to elasticsearch

- Create date:  2018-12-07
- Update date:  2018-12-07
- Version:      1.0

Notes:
==========================        
- v1.0: Initial version
"""
import configparser
import os
import sys

from elasticsearch.helpers import bulk
from pandas import DataFrame, read_sql, Timestamp
import psycopg2 
from sqlalchemy import create_engine

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


config = configparser.ConfigParser()
config.read(sys.path[-1]+'/config.ini')


def engine():
  return create_engine(
    f"postgresql+psycopg2://{config['PG']['USER']}:{config['PG']['PASSWORD']}"
    f"@{config['PG']['HOST']}:{config['PG']['PORT']}/{config['PG']['DB']}", 
    echo=True
  )


ENGINE = engine()


def fetch_sync_log(country, engine):
  switcher = {
    'Guatemala': 'public',
    'Colombia': 'fdw_kingo_co'
  }
  query = (
    "SELECT (sl.region||':pos:'||sl.pos_id) AS system_id, "
      "CASE "
        "WHEN ai.application_instance_type = 1 THEN 'shopkeeper' "
        "WHEN ai.application_instance_type = 2 THEN 'employee' "
        "WHEN ai.application_instance_type = 3 THEN 'distributor' "
        "WHEN ai.application_instance_type IS NULL THEN NULL "
      "END AS instance_type, "
      "ai.shopkeeper_id AS person_id, "
      "ai.employee_id AS employee_id, "
      "max(sl.date) sync_date "
    f"FROM {switcher.get(country)}.sync_log sl "
    "INNER JOIN stream.application_instance ai ON ai.id = sl.pos_id "
    "GROUP BY 1, 2, 3, 4 "
    "ORDER BY 1;"
  )
  
  print(f'Query to be run is {query} \n')

  print(f'{Timestamp.now().isoformat()} - Starting to fetch sync log')
  sync_log = read_sql(query, engine, index_col='system_id')
  sync_log['sync_date'] = sync_log['sync_date'].apply(lambda x: x.isoformat())
  print(f'{Timestamp.now().isoformat()} - Finished fetching sync log')

  return sync_log


def fetch_user_list(engine):
  query = (
    "SELECT id AS employee_id, username AS agent_id "
    "FROM auth.user_account "
    "ORDER BY 1;"
  )
  
  print(f'Query to be run is {query} \n')
  
  return read_sql(query, engine)


def index_sync_log(country):
  if country not in COUNTRY_LIST:
    raise Exception(f'{country} is not a valid country')

  def gendata(df):
    for i in range(len(df)):
      system_id = df.index[i]
      instance_type = df.iloc[i]['instance_type'] 
      person_id = df.iloc[i]['person_id']
      agent_id = df.iloc[i]['agent_id']
      sync_date = df.iloc[i]['sync_date']  
      
      doc = {
        "_index": "sync_log",
        "_type": "_doc",
        "system_id": system_id,
        "country": country,
        "sync_date": sync_date
      }

      if instance_type is not None:
        doc['instance_type'] = instance_type
      
      if person_id is not None:
        doc['person_id'] = str(person_id).replace('-','')

      if isinstance(agent_id, str):
        doc['agent_id'] = agent_id

      yield doc
      
  
  sync_log = fetch_sync_log(country, ENGINE)
  user_list = fetch_user_list(ENGINE)
  sync_df = sync_log.merge(user_list, on='employee_id', how='left')\
    .drop('employee_id', axis=1).set_index(sync_log.index)
  
  print(f'{Timestamp.now().isoformat()} - Starting to index sync log')
  bulk(elastic, gendata(sync_df))
  print(f'{Timestamp.now().isoformat()} - Finished indexind sync log')

if __name__=='__main__':
  index_sync_log('Guatemala')
  # index_sync_log('Colombia') FIXME: