from __future__ import print_function
from pandas import DataFrame
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '1rKNutCZRrLRyUOgYo0DyRvQt41VKAClhnMcmzAO4ptk'
RANGE_NAME = 'Maestro para Tableau!A2:H10000'

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

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_NAME).execute()
    values = result.get('values', [])

    df = DataFrame(values, columns=['community', 'municipality', 'department', 'concat', 'at', 'cs', 'ss', 'clients'])
    df = df.set_index('concat')

    df.to_csv('community_master.csv', sep=',')

    return df

if __name__ == '__main__':
    main()