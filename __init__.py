import configparser
import os
import sys

from elasticsearch import Elasticsearch

CWD = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CWD)

config = configparser.ConfigParser()
config.read(sys.path[-1]+'/config.ini')

if config['ES']['HOST'] is not None and config['ES']['PORT'] is not None:
    elastic = Elasticsearch(
        config['ES']['HOST'],
        port= config['ES']['PORT']
    )

else:
    elastic = Elasticsearch()