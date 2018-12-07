"""
Systems Open by Model
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate reports on systems open by model. The 'open' count is done
in five different ways: 'start', 'end', 'average', 'distinct', and 'weighted'

- Create date:  2018-12-06
- Update date:  2018-12-06
- Version:      1.1

Notes:        
==========================
- v1.0: Uses min_doc_count parameter and pre-populates de obj dictionary 
        with all date x model combinations to guarantee dates are not sparse.
- v1.1: Fixed bug when key didn't exit in df_open_now
"""
from elasticsearch_dsl import Search, Q
from numpy import diff
from pandas import concat, DataFrame, MultiIndex, offsets, Series, Timestamp

from ant_data import elastic
from ant_data.systems import systems_closed__model, systems_opened__model


def search_open_now(country, f=None):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo'), 
        Q('term', open=True)
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'models', 'terms', field='model', exclude=['Kingo Shopkeeper', 'Ant Mobile'], 
    min_doc_count=0
  )

  return s[:0].execute()


def search_distinct(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo')
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
    'models', 'terms', field='model', exclude=['Kingo Shopkeeper', 'Ant Mobile'], 
    min_doc_count=0
  ).bucket('stats', 'children', type='stat') \
  .bucket('date_range', 'filter', Q('range', date={'lte': 'now'})) \
  .bucket(
      'dates', 'date_histogram', field='date', interval=interval, 
      min_doc_count=0 
  ).metric('count', 'cardinality', field='system_id', precision_threshold=40000)
  
  return s[:0].execute()


def search_weighted(country, f=None, interval='month'):
  s = Search(using=elastic, index='systems') \
    .query(
      'bool', filter=[
        Q('term', country=country), Q('term', doctype='kingo')
      ]
    )

  if f is not None:
    s = s.query('bool', filter=f)

  s.aggs.bucket(
      'models', 'terms', field='model', exclude=['Kingo Shopkeeper', 'Ant Mobile'], 
      min_doc_count=0
  ).bucket('stats', 'children', type='stat') \
  .bucket('date_range', 'filter', Q('range', date={'lte': 'now'})) \
  .bucket(
      'dates', 'date_histogram', field='date', interval=interval, 
      min_doc_count=0 
  )
  
  return s[:0].execute()


def df_open_now(country, f=None, interval='month'):
  response = search_open_now(country, f=f)

  models = [x.key for x in response.aggs.models.buckets]

  obj = {x: { 0 } for x in models}

  for model in response.aggs.models.buckets: 
    obj[model.key] = { model.doc_count }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['now']
  )

  if df.empty:
    return df

  df.index.name = 'date'
  df = df.sort_index()

  return df


def df_start(country, f=None, interval='month'):
  df = DataFrame(columns=['model', 'start'])
  open = df_open_now(country, f=f)
  
  if open.empty:
    return df

  opened = systems_opened__model.df(country, f=f, interval=interval)
  closed = systems_closed__model.df(country, f=f, interval=interval)
  
  if opened.empty or closed.empty:
    return df

  models = opened['model'].unique().tolist()
  
  if models == []:
    return df
  
  for model in models:
    merged = opened[opened['model']==model].merge(
      closed[closed['model']==model], on=['date', 'model'], how='outer'
    ).sort_index()
    merged = merged.fillna(0)

    df_model = DataFrame(index=merged.index, columns=['model','start'])
    df_model['model'] = model
    rev_idx = merged.index.tolist()
    rev_idx.reverse()
    
    for i in range(len(rev_idx)):
      if i == 0:
        df_model.loc[rev_idx[i]]['start'] = (
          open['now'].get(model, 0) - merged['opened'].get(rev_idx[i], 0)
          + merged['closed'].get(rev_idx[i], 0)
        )
    
      elif i > 0:
        df_model.loc[rev_idx[i]]['start'] = (
          df_model.loc[rev_idx[i-1]]['start'] - merged['opened'].get(rev_idx[i], 0)
          + merged['closed'].get(rev_idx[i], 0)
        )
    
    df = df.append(df_model)

  df.index.name = 'date'
  df = df.sort_values(['model', 'date'])  
  
  return df


def df_end(country, f=None, interval='month'):
  df = DataFrame(columns=['model', 'end'])
  open = df_open_now(country, f=f)
  
  if open.empty:
    return df

  opened = systems_opened__model.df(country, f=f, interval=interval)
  closed = systems_closed__model.df(country, f=f, interval=interval)
 
  if opened.empty or closed.empty:
    return df

  models = opened['model'].unique().tolist()
  
  if models == []:
    return df
  
  for model in models:
    merged = opened[opened['model']==model].merge(
      closed[closed['model']==model], on=['date', 'model'], how='outer'
    ).sort_index()
    merged = merged.fillna(0)

    df_model = DataFrame(index=merged.index, columns=['model','end'])
    df_model['model'] = model
    rev_idx = merged.index.tolist()
    rev_idx.reverse()
    
    for i in range(len(rev_idx)):
      if i == 0:
        df_model.loc[rev_idx[i]]['end'] = open['now'].get(model, 0)
    
      elif i > 0:
        df_model.loc[rev_idx[i]]['end'] = (
          df_model.loc[rev_idx[i-1]]['end'] 
          - merged['opened'].get(rev_idx[i-1], 0)
          + merged['closed'].get(rev_idx[i-1], 0)
        )
    
    df = df.append(df_model)

  df.index.name = 'date'
  df = df.sort_values(['model', 'date'])

  return df


def df_average(country, f=None, interval='month'):
  df = df_start(country, f=f, interval=interval).merge(
    df_end(country, f=f, interval=interval), on=['date','model'], how='outer').fillna(0)
  
  df['average'] = df[['start', 'end']].mean(axis=1)
  df = df.drop(['start', 'end'], axis=1).sort_values(['model', 'date'])
  
  return df


def df_distinct(country, f=None, interval='month'):

  response = search_distinct(country, f=f, interval=interval)
  
  models = [x.key for x in response.aggs.models.buckets]
  dates = sorted(list(
    {
      y.key_as_string for x in response.aggs.models.buckets
      for y in x.stats.date_range.dates.buckets
    }
  ))

  obj = {(x, y): { 'distinct': 0 } for x in dates for y in models}

  for model in response.aggs.models.buckets:
    for date in model.stats.date_range.dates.buckets:
      obj[(date.key_as_string, model.key)] = { 'distinct': date.count.value }

  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['distinct']
  )

  if df.empty:
    return df

  df = df.rename_axis(['date', 'model'])
  idx = MultiIndex(
    levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
    labels=df.index.labels, names=['date', 'model']
  )
  df = DataFrame(df.values, index=idx, columns=['distinct']).sort_index().reset_index()
  df = df.set_index('date').sort_values(['model', 'date'])

  return df


def df_weighted(country, f=None, interval='month'):
  response = search_weighted(country, f=f, interval=interval)

  models = [x.key for x in response.aggs.models.buckets]
  dates = sorted(list(
    {
      y.key_as_string for x in response.aggs.models.buckets
      for y in x.stats.date_range.dates.buckets
    }
  ))

  obj = {(x, y): { 'weighted': 0 } for x in dates for y in models}

  for model in response.aggs.models.buckets:
    for date in model.stats.date_range.dates.buckets:
      obj[(date.key_as_string, model.key)] = { 'weighted': date.doc_count }
  
  df = DataFrame.from_dict(
    obj, orient='index', dtype='int64', columns=['weighted']
  )

  if df.empty:
    return df

  df = df.rename_axis(['date', 'model'])
  idx = MultiIndex(
    levels=[df.index.levels[0].astype('datetime64'), df.index.levels[1]],
    labels=df.index.labels, names=['date', 'model']
  )
  df = DataFrame(df.values, index=idx, columns=['distinct']).sort_index().reset_index()
  df = df.set_index('date')
  
  for model in df['model'].unique().tolist():

    df_model = df[df['model']==model]
    bucket_len = [x.days for x in diff(df_model.index.tolist())]
    bucket_len.append((Timestamp.now()-df_model.index[-1]).days + 1)
    df_model['distinct'] = df_model['distinct'].div(bucket_len, axis='index')

    df[df['model']==model] = df_model

  df = df.sort_values(['model', 'date'])

  return df


def df(country, method=None, f=None, interval='month'):
  switcher = {
     'start': df_start,
     'end':  df_end,
     'average': df_average,
     'distinct': df_distinct,
     'weighted': df_weighted
   }
  
  if method is not None:
    return switcher.get(method)(country, f=f, interval=interval)

  else:
    start = df_start(country, f=f, interval=interval)
    end = df_end(country, f=f, interval=interval)
    average = df_average(country, f=f, interval=interval)
    distinct = df_distinct(country, f=f, interval=interval)
    weighted = df_weighted(country, f=f, interval=interval)

    if start.empty or end.empty or average.empty or distinct.empty or weighted.empty:  
      return DataFrame(columns=['start', 'end', 'average', 'weighted', 'distinct'])

    return start.merge(end, on=['date', 'model'], how='inner')\
      .merge(average, on=['date', 'model'], how='inner') \
      .merge(distinct, on=['date', 'model'], how='inner')\
      .merge(weighted, on=['date', 'model'], how='inner')
  
