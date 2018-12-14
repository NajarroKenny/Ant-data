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

FNAME_AT = 'roster_at.csv'
FNAME_ATR = 'roster_atr.csv'


def main():
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))

    # Call the Sheets API
    sheet = service.spreadsheets()
    
    result_at = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_AT).execute()
    values_at = result_at.get('values', [])

    df_at = DataFrame(
        values_at, columns=[
            'nombre', 'puesto', 'id', 'cs', 'correo_cs', 'ss', 'correo_ss', 
            'departamento', 'municipio', 'telefono', 'correo', 'ingreso'
            ]
        )
    df_at = df_at.drop(df_at[df_at['nombre']=='Vacante'].index)

    result_atr = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_ATR).execute()
    values_atr = result_atr.get('values', [])
    
    df_atr = DataFrame(
        values_atr, columns=[
            'nombre', 'puesto', 'id', 'cs', 'correo_cs', 'ss', 'correo_ss', 
            'departamento', 'municipio', 'telefono', 'correo', 'ingreso'
            ]
        )
    df_atr = df_atr.drop(df_atr[df_atr['nombre']=='Vacante'].index)
    
    df_at.to_csv(FNAME_AT, sep=',')
    df_atr.to_csv(FNAME_ATR, sep=',')

if __name__ == '__main__':
    main()