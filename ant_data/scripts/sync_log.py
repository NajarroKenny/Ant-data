"""
Sync Log
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


# CWD = os.path.dirname(os.path.realpath(__file__))
# sys.path.append(CWD)

config = configparser.ConfigParser()
config.read(sys.path[-1]+'/config.ini')


def fetch_sync_log():
  query = (
    "SELECT (sl.region||':pos:'||sl.pos_id) AS pos_id, "
      "CASE "
        "WHEN ai.application_instance_type = 1 THEN 'shopkeeper' "
        "WHEN ai.application_instance_type = 2 THEN 'employee' "
        "WHEN ai.application_instance_type = 3 THEN 'distributor' "
        "WHEN ai.application_instance_type IS NULL THEN NULL "
      "END AS instance_type, "
      "ai.shopkeeper_id shopkeeper_id, "
      "ai.employee_id employee_id, "
      "max(sl.date) sync_date "
    "FROM public.sync_log sl "
    "INNER JOIN stream.application_instance ai ON ai.id = sl.pos_id "
    "GROUP BY 1, 2, 3, 4 "
    "ORDER BY 1;"
  )
  
  print(f'Query to be run is {query} \n')

  engine = create_engine(
    f"postgresql+psycopg2://{config['PG']['USER']}:{config['PG']['PASSWORD']}"
    f"@{config['PG']['HOST']}:{config['PG']['PORT']}/{config['PG']['GTM-DB']}", 
    echo=True
  )

  print(f'{Timestamp.now().isoformat()} - Starting to fetch sync log')
  sync_log = read_sql(query, engine, index_col='pos_id')
  sync_log['sync_date'] = sync_log['sync_date'].apply(lambda x: x.isoformat())
  print(f'{Timestamp.now().isoformat()} - Finished fetching sync log')

  return sync_log

def index_sync_log():
  def gendata(sync_log):
    for i in range(len(sync_log)):
      pos_id = sync_log.index[i]
      instance_type = sync_log.iloc[i]['instance_type'] 
      shopkeeper_id = sync_log.iloc[i]['shopkeeper_id']
      employee_id = sync_log.iloc[i]['employee_id']
      sync_date = sync_log.iloc[i]['sync_date']  
      
      doc = {
        "pos_id": pos_id,
        "sync_date": sync_date}

      if instance_type is not None:
        doc['instance_type'] = instance_type
      
      if shopkeeper_id is not None:
        doc['shopkeeper_id'] = shopkeeper_id

      if employee_id is not None:
        doc['employee_id'] = employee_id

      yield {
          "_index": "sync_log",
          "_type": "_doc",
          "doc": doc
      }
  
  sync_log = fetch_sync_log()
  
  print(f'{Timestamp.now().isoformat()} - Starting to index sync log')
  bulk(elastic, gendata(sync_log))
  print(f'{Timestamp.now().isoformat()} - Finished indexind sync log')

