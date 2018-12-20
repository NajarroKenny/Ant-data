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


def search(start=None, end=None, f=None):
    s = Search(using=elastic, index='tasks') \
        .query('term', doctype='task') \
        .query('term', planned=True)

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('remarks', 'terms', field='remarks', size=10000, min_doc_count=1)

    return s[:0].execute()


def df(start=None, end=None, f=None, workflow=None, all=False):
    """Assigned tasks, by type"""
    g = [] if f is None else f[:]

    if workflow is not None:
        g.append(Q('has_child', type='history', query=Q('terms', workflow=workflow)))

    response = search(start=start, end=end, f=g)

    obj = {}

    for remark in response.aggs.remarks.buckets:
        if (
            remark.key.startswith('gestion') or
            remark.key.startswith('gestion 2') or
            remark.key.startswith('gestion 3')
        ):
            tipo = 'gestión'
        elif remark.key.startswith('ticket'):
            tipo = 'ticket'
        elif remark.key.startswith('verificacion'):
            tipo = 'verificación'
        elif remark.key.startswith('tendero venta'):
            tipo = 'tendero venta'
        elif remark.key.startswith('tendero sync'):
            tipo = 'tendero sync'
        elif remark.key.startswith('kingo basico'):
            tipo = 'swap k7 o venta >= 1 semana'
        elif remark.key.startswith('kingo tv'):
            tipo = 'swap a kingo tv'
        elif (
            remark.key.startswith('tecnica') or
            remark.key.startswith('promocion precio') or
            remark.key.startswith('corregir') or
            remark.key.startswith('cliente activo') or
            remark.key.startswith('asignacion especial tecnica')
        ):
            tipo = 'técnica'
        elif remark.key.startswith('instalacion k7'):
            tipo = 'instalación kingo básico'
        elif remark.key.startswith('instalacion k15'):
            tipo = 'instalación kingo luz'
        elif remark.key.startswith('instalacion ktv'):
            tipo = 'instalación kingo TV'
        elif remark.key.startswith('instalacion k100+'):
            tipo = 'instalación kingo hogar'
        elif (
            remark.key.startswith('preventa kingo tv') or
            remark.key.startswith('preventa: k15 tv') or
            remark.key.startswith('preventa: kingo tv')
        ):
            tipo = 'preventa kingo tv'
        elif remark.key.startswith('preventa kingo hogar'):
            tipo = 'preventa kingo hogar'
        else:
            tipo = 'sin tipo'
        obj[tipo] = obj.get(tipo, 0) + remark.doc_count

    df = DataFrame.from_dict(obj, orient='index', columns=['asignadas']).sort_index()

    if df.empty:
        return df

    if not all and 'sin tipo' in df.index:
        df = df.drop('sin tipo')

    df.loc['total'] = df.sum()
    df.index.name = 'tipo de tarea'
    
    return df
