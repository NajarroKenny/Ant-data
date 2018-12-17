from ant_data.stats import days as _days
from ant_data.stats import active as _active
from ant_data.stats import colors as _colors
from ant_data.stats import urate as _urate
from ant_data.stats import urate_buckets as _urate_buckets
from ant_data.stats import days30__model as _days30__model
from ant_data.stats import active30__model as _active30__model
from ant_data.stats import colors30__model as _colors30__model
from ant_data.stats import urate30__model as _urate30__model
from ant_data.stats import urate30_buckets__model as _urate30_buckets__model


def colors(country, f=None, **kwargs):
  return _colors.df(country, f=f, **kwargs)


def colors30__model(country, f=None, **kwargs):
  return _colors30__model.df(country, f=f, **kwargs)


def urate_buckets(country, f=None, **kwargs):
  return _urate_buckets.df(country, f=f, **kwargs)


def urate30_buckets__model(country, f=None, **kwargs):
  return _urate30_buckets__model.df(country, f=f, **kwargs)


def urate(country, f=None, **kwargs):
  return _urate.df(country, f=f, **kwargs)


def urate30__model(country, f=None, **kwargs):
  return _urate30__model.df(country, f=f, **kwargs)


def active(country, f=None, **kwargs):
  return _active.df(country, f=f, **kwargs)


def active30__model(country, f=None, **kwargs):
  return _active30__model.df(country, f=f, **kwargs)


def days(country, f=None, **kwargs):
  return _days.df(country, f=f, **kwargs)


def days30__model(country, f=None, **kwargs):
  return _days30__model.df(country, f=f, **kwargs)