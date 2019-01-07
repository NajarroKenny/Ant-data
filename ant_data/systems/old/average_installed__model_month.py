from ant_data import elastic

from .days_installed_month import df as _df

def df(q = None):
    df = _df(q)
    df = df.div(df['days_in_month'], axis=0)
    df = df.drop(['days_in_month'], axis=1)
    return df