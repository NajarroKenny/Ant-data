"""
Code count per day
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on code counts per day at different grouping 
levels

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

    The query is a multi-level grouping executed on the 'codes' index. The date
    histogram at the DAY level is the lowest grouping level, always present. 
    The other grouping levels are:
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
            .bucket('days', 'date_histogram', field='datetime', interval='day'),
        2: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .bucket('days', 'date_histogram', field='datetime', interval='day'),
        3: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .bucket('sales', 'terms', field='sale', size=1000) \
            .bucket('days', 'date_histogram', field='datetime', interval='day'),
        4: lambda: s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
            .bucket('doctypes', 'terms', field='doctype', size=1000) \
            .bucket('sales', 'terms', field='sale', size=1000) \
            .bucket('plans', 'terms', field='plan', size=1000) \
            .bucket('days', 'date_histogram', field='datetime', interval='day')
    }
    switcher.get(group_level, s)()
    return s[:0].execute()

def df(q=None, group_level=4):
    """Returns dataframe with code count at different grouping levels.

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
        group_level parameter and a single column named 'count'. The index
        levels are:
        - Level 1: 'cat'
        - Level 2: 'doctype'
        - Level 3: 'sale'
        - Level 4: 'plan'
        - Level 5: 'day'
    """
    response = search(q, group_level)

    data = []

    for cat in response.aggregations.cats.buckets:
        if group_level < 2:
            # breakpoint()
            for day in cat.days.buckets:
                data.append({ 
                    'cat': cat.key, 'day': day.key_as_string, 
                    'count': day.doc_count 
                })
        
        else:
            for doctype in cat.doctypes.buckets:
                if group_level < 3:
                    for day in doctype.days.buckets:
                        data.append({ 
                            'cat': cat.key, 'doctype': doctype.key, 
                            'day': day.key_as_string, 
                            'count': day.doc_count 
                        })

                else:
                    for sale in doctype.sales.buckets:
                        if group_level < 4:
                            for day in sale.days.buckets:
                                data.append({ 
                                    'cat': cat.key, 'doctype': doctype.key, 
                                    'sale': sale.key, 
                                    'day': day.key_as_string, 
                                    'count': day.doc_count 
                                })
                        
                        else:
                            for plan in sale.plans.buckets:
                                for day in plan.days.buckets:
                                    data.append({ 
                                        'cat': cat.key, 'doctype': doctype.key, 
                                        'sale': sale.key, 'plan': plan.key, 
                                        'day': day.key_as_string, 
                                        'count': day.doc_count 
                                    })

    df = DataFrame(json_normalize(data))
    df['day'] = df['day'].astype('datetime64')
    
    switcher = {
        1: lambda: df.set_index(['cat', 'day']).sort_index(),
        2: lambda: df.set_index(['cat', 'doctype', 'day']).sort_index(),
        3: lambda: df.set_index(['cat', 'doctype', 'sale', 'day']).sort_index(),
        4: lambda: df.set_index(['cat', 'doctype', 'sale', 'plan', 'day']).sort_index()
    }

    return switcher.get(group_level, lambda: DataFrame)()