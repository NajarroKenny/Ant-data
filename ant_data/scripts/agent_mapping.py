"""
Agent Mapping
==========================
Creates an dictionary to map out-of-sync agent Ant Id's to Task/Assignment Id's.
The dictionary is saved to ant_data/static/AGENT_MAPPING.py

The dictionary is created from a CSV file in the same directory as this script

- Create date:  2018-12-17
- Update date:  2018-12-17
- Version:      1.0

Notes:
==========================        
- v1.0: Initial version
"""
import csv
from pkg_resources import resource_filename


SOURCE = 'agent_mapping.csv'
DEST_PATH = '../static/AGENT_MAPPING.py' 
DEST = resource_filename(__name__, DEST_PATH)
FHEADER = 'AGENT_MAPPING = { \n'
FFOOTER = '}'

with open(DEST, 'w') as fh:
  fh.write(FHEADER)
  with open(SOURCE, newline='') as csvf:
    csv_reader = csv.reader(csvf, delimiter=',')
    next(csv_reader, None)
    for row in csv_reader:
      fh.write(f"\t'{row[0]}': '{row[1]}',\n")
  fh.write(FFOOTER)

