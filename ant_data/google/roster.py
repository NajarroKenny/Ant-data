"""
Roster
============================
Leverages Google Sheet's API to fetch Kingo's Roster and save it as a csv
to ant_data/google/roster_at.csv and ant_data/google/roster_atr.csv

- Create date:  2018-12-14
- Update date:  2018-12-14
- Version:      1.1

Notes:
============================      
- v1.0: Initial version
- v1.1: Split between ATs and ATRs
"""
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from pandas import DataFrame

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
# The ID and range of a sample spreadsheet.
# SPREADSHEET_ID = '1UjLlBMpf7UqOWe2Icp3kJuOHc0zN0S_U365o4-AnMQw' FIXME:
SPREADSHEET_ID = '1WXLG241suRmBKKGJyyu43-T5DYErPrpr0GBG0T8ogq0'
RANGE_AT = 'AT´S!B2:M'
RANGE_ATR = 'ATR´S!B2:M'
RANGE_CS_SS = 'CS - SS!B2:J'

FNAME_AGENTS = 'roster_agents.csv'
FNAME_COORDINATORS = 'roster_coordinators.csv'
FNAME_SUPERVISORS = 'roster_supervisors.csv'    

AT_ROLES = ['asesor técnico', 'asesor técnico comodín']
IT_ROLES = ['instalador']
ATR_ROLES = ['asesor técnico rutero', 'atr aperturador']
COORDINATOR_ROLES = ['coordinador de servicio', 'coordinador de servicio comodín',
'coordinador de instalación', 'coordinador kingo cash']
SUPERVISOR_ROLES = ['supervisor comercial', 'supervisor it']

def assign_role_id(role):


    if role.lower() in AT_ROLES:
        return 'at'
    elif role.lower() in IT_ROLES:
        return 'it'
    elif role.lower() in ATR_ROLES:
        return 'atr'
    elif role.lower() in COORDINATOR_ROLES:
        return 'coordinator'
    elif role.lower() in SUPERVISOR_ROLES:
        return 'supervisor'

def auth():
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))

    return service

def agents():
    AGENTS_COLS = [
        'name', 'role', 'system_id', 'coordinator', 'coordinator_id', 
        'supervisor', 'supervisor_id', 'departament', 'municipality', 
        'phone', 'agent_id', 'start_date'
    ]
    
    service = auth()

    # Call the Sheets API
    sheet = service.spreadsheets()
    
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_AT).execute()
    values = result.get('values', [])

    df = DataFrame(values, columns=AGENTS_COLS)

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_ATR).execute()
    values = result.get('values', [])
    
    df = df.append(DataFrame(values, columns=AGENTS_COLS))
    df = df.drop(df[df['name']=='Vacante'].index)
    df['role_id'] = df['role'].apply(assign_role_id)
    COL_ORDER = AGENTS_COLS[0:2]+['role_id']+AGENTS_COLS[2:-1]
    df = df[COL_ORDER]
    df = df.set_index('agent_id')

    df.to_csv(FNAME_AGENTS, sep=',')

def coordinators():
    COORDINATOR_COLS = [
        'name', 'role', 'system_id', 'supervisor', 'supervisor_id', 'region',
        'phone', 'coordinator_id', 'start_date'
    ]

    service = auth()

    # Call the Sheets API
    sheet = service.spreadsheets()
    
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_CS_SS).execute()
    values = result.get('values', [])

    df = DataFrame(values, columns=COORDINATOR_COLS)
    df = df[df['role'].apply(lambda x: x.lower()).isin(COORDINATOR_ROLES)]
    df['role_id'] = df['role'].apply(assign_role_id)
    COL_ORDER = COORDINATOR_COLS[0:2]+['role_id']+COORDINATOR_COLS[2:-1]
    df = df[COL_ORDER]
    df = df.set_index('coordinator_id')

    df.to_csv(FNAME_COORDINATORS, sep=',')

def supervisors():
    SUPERVISOR_COLS = [
        'name', 'role', 'system_id', 'boss', 'boss_id', 'region',
        'phone', 'supervisor_id', 'start_date'
    ]

    service = auth()

    # Call the Sheets API
    sheet = service.spreadsheets()
    
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_CS_SS).execute()
    values = result.get('values', [])

    df = DataFrame(values, columns=SUPERVISOR_COLS)
    df = df[df['role'].apply(lambda x: x.lower()).isin(SUPERVISOR_ROLES)]
    df['role_id'] = df['role'].apply(assign_role_id)
    COL_ORDER = SUPERVISOR_COLS[0:2]+['role_id']+SUPERVISOR_COLS[2:-1]
    df = df[COL_ORDER]
    df = df.set_index('supervisor_id')

    df.to_csv(FNAME_SUPERVISORS, sep=',')


if __name__ == '__main__':
    agents()
    coordinators()
    supervisors()