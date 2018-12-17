from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize
import pandas as pd

def search(country, f=None):
    s = Search(using=elastic, index='people') \
      .query('term', country=country) \
      .query('term', doctype='client') \
      .query('term', open=True)

    if f is not None:
        s = s.query('bool', filter=f)

    return s


def df(country, f=None):
    s = search(country, f=f)

    rows = []
    for hit in s.scan():
        doc = hit.to_dict()
        community = doc.get('community', {})
        employees = community.get('employees', {})
        at = employees.get('at', {})
        atr = employees.get('atr', {})
        gps = doc.get('gps_point', {})
        community_gps = community.get('gps_point', {})
        stats = doc.get('stats', {})

        row = {
            'client_id': doc.get('person_id', ''),
            'at_id': at.get('agent_id', ''),
            'c_at_id': at.get('coordinator_id', ''),
            's_at_id': at.get('supervisor_id', ''),
            'atr_id': atr.get('agent_id', ''),
            'c_atr_id': atr.get('coordinator_id', ''),
            's_atr_id': atr.get('supervisot_id', ''),
            'lat': gps.get('lat', ''),
            'lon': gps.get('lon', ''),
            'community_id': community.get('community', ''),
            'community': community.get('community_id', ''),
            'municipality': community.get('municipality', ''),
            'department': community.get('department', ''),
            'community_lat': community.get('lat', ''),
            'community_lon': community.get('lon', ''),
            'opened': doc.get('opened', ''),
            'sync_data': doc.get('sync_date', ''),
            'active_date': doc.get('active_date', ''),
            'update_date': doc.get('update_date', ''),
            'color30': stats.get('color30', ''),
            'color60': stats.get('color60', ''),
            'color90': stats.get('color90', ''),
            'color180': stats.get('color180', ''),
            'color360': stats.get('color360', ''),
            'ur30': stats.get('ur30', ''),
            'ur60': stats.get('ur60', ''),
            'ur90': stats.get('ur90', ''),
            'ur180': stats.get('ur180', ''),
            'ur360': stats.get('ur360', ''),
            'days30': stats.get('days30', ''),
            'days60': stats.get('days60', ''),
            'days90': stats.get('days90', ''),
            'days180': stats.get('days180', ''),
            'days360': stats.get('days360', ''),
            'idle': doc.get('idle', '')

        }

        rows.append(row)

    df = DataFrame(rows)
    if not df.empty:
        df = df.set_index('client_id')
        df = df[[
          'at_id',
          'c_at_id',
          's_at_id',
          'atr_id',
          'c_atr_id',
          's_atr_id',
          'community_id',
          'community',
          'municipality',
          'department',
          'opened',
          'sync_data',
          'active_date',
          'update_date',
          'color30',
          'color60',
          'color90',
          'color180',
          'color360',
          'ur30',
          'ur60',
          'ur90',
          'ur180',
          'ur360',
          'days30',
          'days60',
          'days90',
          'days180',
          'days360',
          'idle'
        ]]

    return df


def csv(country, filename, f=None):
  cl = df(country, f=f)
  cl.to_csv(filename, sep=',')


if __name__ == '__main__':
    csv(country, filename, f=None):