"""
Code value
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on code value at different grouping levels

    - Level 1: Code Category
    - Level 2: Code Doctype
    - Level 3: Code Sale type
    - Level 4: Code Plan
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize

from ant_data import elastic


def search(q=None, group_level=4):
    """Instantiates and builds a search object to perform the respective query.

    The query is a multi-level grouping executed on the 'codes' index. The
    grouping levels are:
    - Level 1: Code Category
    - Level 2: Code Doctype
    - Level 3: Code Sale type
    - Level 4: Code Plan

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        group_level (int, optional): Grouping level to which perform the search.

    Returns:
        elasticsearch-dsl search object response.
    """
    s = Search(using=elastic, index='codes') \
        .query()

    if q is not None:
        s = s.query(q)

    switcher = {
        1: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .metric('value', 'sum', field='value', missing=0),
        2: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .metric('value', 'sum', field='value', missing=0),
        3: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .bucket('sales', 'terms', field='sale', size=1000) \
            .metric('value', 'sum', field='value', missing=0),
        4: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .bucket('sales', 'terms', field='sale', size=1000) \
            .bucket('plans', 'terms', field='plan', size=1000) \
            .metric('value', 'sum', field='value', missing=0)
    }
    switcher.get(group_level, s)()

    # s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
    #     .bucket('doctypes', 'terms', field='doctype', size=1000) \
    #     .bucket('sales', 'terms', field='sale', size=1000) \
    #     .bucket('plans', 'terms', field='plan', size=1000) \
    #     .metric('value', 'sum', field='value', missing=0)

    return s[:0].execute()

def df(q=None, group_level=4):
    """Returns dataframe with code value at different grouping levels.

    The data is grouped at the following levels:
    - Level 1: Code Category
    - Level 2: Code Doctype
    - Level 3: Code Sale type
    - Level 4: Code Plan

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        group_level (int, optional): Level to which group the data.

    Returns:
        Pandas DataFrame with a multi-level index that will depend on the
        group_level parameter and a single column named 'value'. The index
        levels are:
        - Level 1: 'cat'
        - Level 2: 'doctype'
        - Level 3: 'sale'
        - Level 4: 'plan'
    """
    response = search(q, group_level)

    data = []

    for cat in response.aggregations.cats.buckets:
        if group_level < 2:
            data.append({
                'cat': cat.key, 'count': cat.value.value
            })

        else:
            for doctype in cat.doctypes.buckets:
                if group_level < 3:
                    data.append({
                        'cat': cat.key, 'doctype': doctype.key,
                        'count': doctype.value.value
                    })

                else:
                    for sale in doctype.sales.buckets:
                        if group_level < 4:
                            data.append({
                                'cat': cat.key, 'doctype': doctype.key,
                                'sale': sale.key, 'count': sale.value.value
                            })

                        else:
                            for plan in sale.plans.buckets:
                                data.append({
                                    'cat': cat.key, 'doctype': doctype.key,
                                    'sale': sale.key, 'plan': plan.key,
                                    'count': plan.value.value
                                })

    df = DataFrame(json_normalize(data))

    switcher = {
        1: lambda: df.set_index(['cat']).sort_index(),
        2: lambda: df.set_index(['cat', 'doctype']).sort_index(),
        3: lambda: df.set_index(['cat', 'doctype', 'sale']).sort_index(),
        4: lambda: df.set_index(['cat', 'doctype', 'sale', 'plan']).sort_index()
    }

    return switcher.get(group_level, lambda: DataFrame)()

    # for cat in response.aggregations.cats.buckets:
    #     for doctype in cat.doctypes.buckets:
    #         for sale in doctype.sales.buckets:
    #             for plan in sale.plans.buckets:
    #                 data.append({ 'cat': cat.key, 'doctype': doctype.key, 'sale': sale.key, 'plan': plan.key, 'value': plan.value.value })

    # df = DataFrame(json_normalize(data))
    # df = df.set_index(['cat', 'doctype', 'sale', 'plan']).sort_index()

    # return df