import pandas as pd
import pkg_resources

IVA = {
    'Guatemala': 0.12,
    'Colombia': 0.17
}

path = 'fx.csv'
filepath = pkg_resources.resource_filename(__name__, path)
FX = pd.read_csv(filepath, index_col='date',
                 dtype={'gtq': 'float64', 'cop': 'float64'}, parse_dates=True)
