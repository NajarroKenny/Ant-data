#TODO: Review if the additional searches don't overlap
"""
Task Types
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task types.

- Create date:  2018-12-21
- Update date:  2018-12-26
- Version:      1.2

Notes:
============================
- v1.0: Initial version
- v1.1: Better handling of empty cases
- v1.2: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search_additionals(start=None, end=None, f=None):
    """Searches generic additional tasks"""
    s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
        .query('term', doctype='task') \
        .query('bool', should=[
            Q('term', planned=False), ~Q('exists', field='planned')
         ]) \
        .exclude('has_child', type='history', query=Q('term', workflow='install'))

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    return s[:0].execute().hits.total


def search_additional_installations(start=None, end=None, f=None):
    "Searches additional installations based on workflow type==install"
    s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
        .query('term', doctype='task') \
        .query('bool', should=[
            Q('term', planned=False), ~Q('exists', field='planned')
         ]) \
        .query('has_child', type='history', query=Q('term', workflow='install'))

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    return s[:0].execute().hits.total


def search_additional_shopkeepers(start=None, end=None, f=None):
    """Searches additional shopkeeper tasks based on model"""
    s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
        .query('term', doctype='task') \
        .query('bool', should=[
            Q('term', planned=False), ~Q('exists', field='planned')
         ]) \
        .query('term', model='Kingo Shopkeeper')

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    return s[:0].execute().hits.total


def df(start=None, end=None, f=None):
    """Additional tasks.

    1. Generic additionals tasks
    2. Additonal installations
    3. Additional shopkeeper tasks
    """
    additionals = search_additionals(start, end, f)
    additional_installations = search_additional_installations(
        start, end, f
    )
    additional_shopkeepers = search_additional_shopkeepers(start, end, f)

    obj = {
        'tarea adicional': additionals,
        'instalación adicional': additional_installations,
        'venta a tendero': additional_shopkeepers
        }

    df = DataFrame.from_dict(obj, orient='index', columns=['conteo']).sort_index()

    df.loc['total'] = df.sum()
    df.index.name = 'acción realizada'

    return df

