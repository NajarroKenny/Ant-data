"""
Tasks action list
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task actions based on a given action list.

- Create date:  2018-12-10
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def search_must(country, actions, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')
    
    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='task') \
        .query('has_child', type='history', query=Q('terms', workflow=actions))

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('agents', 'terms', field='agent_id', size=10000) \
        .bucket(
        'dates', 'date_histogram', field='due', interval=interval
    )

    return s[:0].execute()


def search_must_not(country, actions, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')
    
    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='task') \
        .query('bool', should=[
            Q('bool', must_not=Q('has_child', type='history', query=Q())),
            Q('has_child', type='history', query=~Q('terms', workflow=actions))
        ])
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('agents', 'terms', field='agent_id', size=10000) \
        .bucket(
        'dates', 'date_histogram', field='due', interval=interval,
        min_doc_count=0
    )

    return s[:0].execute()


def df(country, actions, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')
    
    must = search_must(country, actions, f=f, interval=interval)
    must_not = search_must_not(country, actions, f=f, interval=interval)

    obj = {}
    for agent in must.aggregations.agents.buckets:
        if not agent.key in obj:
            obj[agent.key] = {}
        for interval in agent.dates.buckets:
            if not interval.key_as_string in obj[agent.key]:
                obj[agent.key][interval.key_as_string] = {}
            obj[agent.key][interval.key_as_string]['should'] = interval.doc_count

    for agent in must_not.aggregations.agents.buckets:
        if not agent.key in obj:
            obj[agent.key] = {}
        for interval in agent.dates.buckets:
            if not interval.key_as_string in obj[agent.key]:
                obj[agent.key][interval.key_as_string] = {}
            obj[agent.key][interval.key_as_string]['must_not'] = interval.doc_count

    df = DataFrame.from_dict({(i, j): obj[i][j]
                              for i in obj.keys()
                              for j in obj[i].keys()},
                             orient='index')

    if df.empty:
        return df

    df.index = df.index.set_levels(
        df.index.levels[1].astype('datetime64'), level=1)

    df.index = df.index.set_names('agent_id', level=0)
    df.index = df.index.set_names('date', level=1)
    df = df.fillna(0).astype('int64')
    df = df.swaplevel(1, 0).reset_index()
    df = df.set_index('date')
    df['total'] = df.sum(axis=1)

    return df
