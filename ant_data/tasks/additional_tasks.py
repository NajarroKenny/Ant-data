"""
Task Types
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task types.

- Create date:  2018-12-19
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def search_additionals(start=None, end=None, f=None):
    s = Search(using=elastic, index='tasks') \
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

    return s[:0].execute()


def search_additional_installations(start=None, end=None, f=None):
    s = Search(using=elastic, index='tasks') \
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

    return s[:0].execute()


def search_additional_shopkeepers(start=None, end=None, f=None):
    s = Search(using=elastic, index='tasks') \
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

    return s[:0].execute()


def df(start=None, end=None, f=None):
    """Additional tasks.

    1. Generic additionals tasks
    2. Additonal installations
    3. Additional shopkeeper tasks
    """

    additionals = search_additionals(start, end, f).hits.total
    additional_installations = search_additional_installations(
        start, end, f
    ).hits.total
    additional_shopkeepers = search_additional_shopkeepers(start, end, f).hits.total

    obj = {
        'tarea adicional': additionals,
        'instalación adicional': additional_installations,
        'venta a tendero': additional_shopkeepers
        }

    df = DataFrame.from_dict(obj, orient='index', columns=['conteo']).sort_index()

    if df.empty:
        return df

    df.loc['total'] = df.sum()
    df.index.name = 'acción realizada'

    return df

