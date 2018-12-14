"""
Roster
============================
Leverages Google Sheet's API to fetch Kingo's Roster and save it as a csv
to ant_data/google/roster.csv

- Create date:  2018-12-14
- Update date:  2018-12-14
- Version:      1.0

Notes:
============================      
- v1.0: Initial version
"""
import sys

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from pandas import DataFrame

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1UjLlBMpf7UqOWe2Icp3kJuOHc0zN0S_U365o4-AnMQw'
RANGE1 = 'AT´S!B2:K'
RANGE2 = 'ATR´S!B2:K'
FNAME = 'roster.csv'


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
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE1).execute()
    values = result.get('values', [])

    df = DataFrame(
        values, columns=[
            'nombre', 'puesto', 'id', 'cs', 'ss', 'departamento', 'municipio', 
            'telefono', 'correo', 'ingreso'
            ]
        )
    breakpoint()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE2).execute()
    values = result.get('values', [])
    df = df.append(DataFrame(
        values, columns=[
            'nombre', 'puesto', 'id', 'cs', 'ss', 'departamento', 'municipio', 
            'telefono', 'correo', 'ingreso'
            ]
        ))

    df.to_csv(FNAME, sep=',')

if __name__ == '__main__':
    main()