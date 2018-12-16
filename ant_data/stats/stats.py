from ant_data.stats import days as _days
from ant_data.stats import active as _active
from ant_data.stats import colors as _colors
from ant_data.stats import urates as _urates
from ant_data.stats import days30__model as _days30__model
from ant_data.stats import active30__model as _active30__model
from ant_data.stats import colors30__model as _colors30__model
from ant_data.stats import urates30__model as _urates30__model


def colors(country, f=None, **kwargs):
  return _colors.df(country, f=f, **kwargs)


def colors30__model(country, f=None, **kwargs):
  return _colors30__model.df(country, f=f, **kwargs)


def urates(country, f=None, **kwargs):
  return _urates.df(country, f=f, **kwargs)


def urates30__model(country, f=None, **kwargs):
  return _urates30__model.df(country, f=f, **kwargs)


def active(country, f=None, **kwargs):
  return _active.df(country, f=f, **kwargs)


def active30__model(country, f=None, **kwargs):
  return _active30__model.df(country, f=f, **kwargs)


def days(country, f=None, **kwargs):
  return _days.df(country, f=f, **kwargs)


def days30__model(country, f=None, **kwargs):
  return _days30__model.df(country, f=f, **kwargs)