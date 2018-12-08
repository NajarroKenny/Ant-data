from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series


def search(country, type, f=None, interval='month'):
    s = Search(using=elastic, index='people') \
        .query('bool', filter=[Q('term', country=country),
                               Q('term', doctype='client')])

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket(
        'dates', 'date_histogram', field=type, interval=interval, 
        min_doc_count=0
    ).bucket(
        'communities', 'terms', field='community.community_id', min_doc_count=0
    )

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, 'opened', f=f, interval=interval)
    obj = {}

    for date in response.aggregations.dates.buckets:
        for community in date.communities.buckets:
            obj[(date.key_as_string, community.key)] = {
                'opened': community.doc_count}

    response = search(country, 'closed', f=f, interval=interval)

    for date in response.aggregations.dates.buckets:
        for community in date.communities.buckets:
            obj[(date.key_as_string, community.key)] = obj.get(
                (date.key_as_string, community.key), {})
            obj[(date.key_as_string, community.key)
                ]['closed'] = community.doc_count

    df = DataFrame.from_dict(
        obj, orient='index', dtype='int64', columns=['opened', 'closed'])

    if df.empty:
        return df

    df = df.rename_axis(['date', 'community_id'])
    df = df.fillna(0).astype('int64')
    idx = MultiIndex(
        levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
        labels=df.index.labels, names=['date', 'community_id']
    )
    df = DataFrame(df.values, index=idx, columns=['opened', 'closed']).swaplevel(
        'date', 'community_id').sort_index()
    df = df[(df.T != 0).any()]

    return df
