from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize
import pandas as pd
from ..static.FINANCE import IVA


def search(country, f=None):
    s = Search(using=elastic, index='communities') \
        .query('bool', filter=[
            Q('term', doctype='community'),
            Q('term', country=country)
        ])

    if f is not None:
        s = s.query('bool', filter=f)

    return s


def df(country, f=None):
    s = search(country, f=f)

    rows = []
    for hit in s.scan():
        row = {
            'id': hit.id,
            'community': hit.community,
            'municipality': hit.municipality,
            'department': hit.department
        }
        if 'gps_point' in hit:
            row['lat'] = hit.gps_point.lat
            row['lon'] = hit.gps_point.lon

        rows.append(row)

    df = DataFrame(rows)
    if not df.empty:
        df = df.set_index('id')
        df = df[['lat', 'lon', 'community', 'municipality', 'department']]

    return df
