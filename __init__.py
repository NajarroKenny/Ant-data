import configparser
import os
import sys

from elasticsearch import Elasticsearch

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CWD)

config = configparser.ConfigParser()
config.read(sys.path[-1]+'/config.ini')

if config['ES']['HOST'] != '' and config['ES']['PORT'] != '':
    print(f"Elasticsearch host is {config['ES']['HOST']}")
    print(f"Elastcisearch port is {config['ES']['PORT']}")

    elastic = Elasticsearch(
        config['ES']['HOST'],
        port= config['ES']['PORT']
    )

else:
    elastic = Elasticsearch()