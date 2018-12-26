
"""
Ant Data Init File
==========================
Indexes roster and community master information in hierarchy index

- Create date:  2018-11-28
- Update date:  2018-12-26
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.1: 
- v1.2: Renamed config to uppercase. Added case where timeout might not be
        existent
"""
import configparser
import os

from elasticsearch import Elasticsearch


ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')

if CONFIG['ES']['HOST'] != '' and CONFIG['ES']['PORT'] != '':
  print(f"Elasticsearch host is {CONFIG['ES']['HOST']}")
  print(f"Elasticsearch port is {CONFIG['ES']['PORT']}")
  print(f"Elasticsearch timeout is {CONFIG['ES']['TIMEOUT']}")

  elastic = Elasticsearch(
    CONFIG['ES']['HOST'],
    port= CONFIG['ES']['PORT'],
    timeout = float(CONFIG['ES']['TIMEOUT'])
  )

elif CONFIG['ES']['HOST'] != '':
  print(f"Elasticsearch timeout is {CONFIG['ES']['TIMEOUT']}")
  elastic = Elasticsearch(timeout = float(CONFIG['ES']['TIMEOUT']))

else:
    print(
       f'No configuration parameters passed to Elasticsearch. Instanting '
       f'client with default parameters.'
    )
    elastic = Elasticsearch()