from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, MultiIndex, Series


def search(country, type, f=None, interval='month'):
    s = Search(using=elastic, index='installs') \
         .query(Q('term', doctype='install') & ~Q('term', model='Kingo Shopkeeper'))

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram', field=type, interval=interval) \
        .bucket('models', 'terms', field='model')

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, 'opened', f=f, interval=interval)
    obj = {}

    for date in response.aggregations.dates.buckets:
        for model in date.models.buckets:
            obj[(date.key_as_string, model.key)] = {
                'opened': model.doc_count}

    response = search(country, 'closed', f=f, interval=interval)

    for date in response.aggregations.dates.buckets:
        for model in date.models.buckets:
            obj[(date.key_as_string, model.key)] = obj.get(
                (date.key_as_string, model.key), {})
            obj[(date.key_as_string, model.key)
                ]['closed'] = model.doc_count

    df = DataFrame.from_dict(
        obj, orient='index', dtype='int64', columns=['opened', 'closed'])

    if df.empty:
        return df

    df = df.rename_axis(['date', 'model'])
    idx = MultiIndex(
        levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
        labels=df.index.labels, names=['date', 'model']
    )
    df = DataFrame(df.values, index=idx, columns=['opened', 'closed']).swaplevel(
        'date', 'model').sort_index()

    dates = df.index.levels[1].unique()
    models = df.index.levels[0].values
    bfi = MultiIndex.from_product([models, dates], names=['model', 'date'])
    bfdf = DataFrame(index=bfi)
    bfdf = bfdf.reset_index().merge(df.reset_index(), how='left', on=['model', 'date'])
    bfdf = bfdf.fillna(0)
    bfdf['open'] = bfdf.groupby(['model'])['opened'].cumsum() \
        - bfdf.groupby(['model'])['closed'].cumsum()
    df = bfdf[(bfdf[['opened', 'closed', 'open']].T != 0).any()]
    df = df.set_index('date')

    return df
