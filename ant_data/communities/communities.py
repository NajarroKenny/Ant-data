from ant_data.communities import communities_gps as _communities_gps

def communities_gps(country, f=None):
  return _communities_gps.df(country, f)
