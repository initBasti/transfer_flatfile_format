"""
    Author: Sebastian Fricke, Panasiam
    Date: 2020-08-25
    License: GPLv3

    Read and write data from/to the google sheets API.
"""
import sys
import os
import math
import pickle
import pandas
import numpy as np

from itertools import islice

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
LEN_ALPHABET = 26

USER = os.getlogin()
if sys.platform == 'linux':
    DATA_DIR = os.path.join('/', 'home', str(f'{USER}'),
                               '.transfer_flatfile_format_data/')
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
elif sys.platform == 'win32':
    DATA_DIR = os.path.join('C:\\', 'Users', str(f'{USER}'),
                               '.transfer_flatfile_format_data/')
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)
CREDENTIAL_PATH = os.path.join(DATA_DIR, '.credentials.json')
TOKEN_PATH = os.path.join(DATA_DIR, 'token.pickle')

def build_sheet_range(column, max_row, range_column=''):
    """
        Specify the area within the google sheet used for reading/writing.

        Parameters:
            column [String] : Start column on the sheet ('A', 'B' etc.)
            max_row [Integer] : Amount of rows for the range
            range_column [String] : End column on the sheet
                                (To create a horizontal-vertical range (A1:E2))

        Return:
            [String] - Example: 'A1:E5' (A1, A2, ... , E4, E5)
    """
    if max_row:
        if not range_column:
            return str(f'{column}1:{column}{max_row}')
        return str(f'{column}1:{range_column}{max_row}')
    print("No specified amount of rows to read, default to range: {0}"
          .format('A1:E20'))
    print("Check the config.ini file to adjust the range")
    return 'A1:E20'

def fill_up_values(val, maximum):
    for _ in range(len(val), maximum):
        val.append('')
    return val

def build_column_name(column_enum):
    a_const = ord('A')
    quotient = math.floor(column_enum/LEN_ALPHABET)

    if column_enum > LEN_ALPHABET - 1:
        first = chr(a_const + quotient - 1)
        second = chr(a_const + (column_enum - (quotient * LEN_ALPHABET)))
        return first + second
    return chr(a_const + column_enum)

def write_chunks(data, size=25):
    """
        Split the data for the batch write to the google sheet into smaller
        chunks. This is useful to prevent write request from being to large.

        Parameter:
            data [List]     - Data containing dictionaries with ranges and
                              values for a write
            size [int]      - chunk size

        Return:
            [List]          - Sub-list of size SIZE
    """
    for i in range(0, len(data), size):
        yield [k for k in islice(iter(data), size)]

def get_google_credentials():
    """
        Check if the token.pickle file contains valid credentials
        if that is not the case get them manually.
        TODO: find method for cronjob to notify administrator about
        problem.

        Return:
            [Google Sheet credentials]
    """
    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIAL_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        try:
            with open(TOKEN_PATH, 'wb') as token:
                pickle.dump(creds, token)
        except PermissionError as err:
            msg = '''Initial setup needs root permissions (sudo ..)'''
            print(msg)
            log.update_log(error=str(f"{msg}\n{err}"),
                           log_folder=mappings.log_location)

    return creds

def read_google_sheet(creds, sheet_id):
    """
        Open the sheet with the @sheet_id, check if the column names
        are as expected, pull the data and transform to pandas dataframe.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String] : Identification of the google sheet

        Return:
            [DataFrame] : containing ID(SKU) and specified column
    """
    max_val = 0
    service = build('sheets', 'v4', credentials=creds)
    sheet_dict = {}
    sheet_range = [build_sheet_range(column='A', max_row='2400',
                                     range_column='FH')]

    sheet = service.spreadsheets()
    result = sheet.values().batchGet(spreadsheetId=sheet_id,
                                     ranges=sheet_range).execute()
    ranges = result['valueRanges']
    if not ranges:
        print('No data found')

    for data in ranges:
        values = data['values']
        column_names = values[2]
        for i in range(3, len(values)):
            if not values[i][1]:
                continue
            # Only take rows which are not filled out
            if len(values[i]) > 2:
                if values[i][2] and values[i][5]:
                    continue

            if len(values[i]) < len(column_names):
                values[i] = fill_up_values(val=values[i],
                                           maximum=len(column_names))
            for index, column in enumerate(column_names):
                if index >= len(values):
                    continue
                if not column in sheet_dict.keys():
                    sheet_dict[column] = []
                if not values[i][index]:
                    sheet_dict[column].append('')
                else:
                    sheet_dict[column].append(values[i][index])
            if not 'index' in sheet_dict.keys():
                sheet_dict['index'] = []
            sheet_dict['index'].append(i)

    frame = pandas.DataFrame(sheet_dict, columns=column_names + ['index'])

    return frame

def write_google_sheet(creds, sheet_id, frame):
    """
        Enter a specific value to column:row coordinate.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String] : Identification of the google sheet
            frame [DataFrame] : difference between source and target
            key [String]      : header name of the column to update

        Return:
            [Bool] True success / False failure/empty
    """
    data = []
    data_cols = ['range', 'values']

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    generator = frame.iterrows()
    for item in generator:
        for i, col in enumerate([x for x in frame.columns if x is not 'index']):
            if col == 'item_sku':
                continue
            range_name = build_column_name(i) + str(item[1]['index']+1)
            values = [[str(item[1][col]).replace(str(np.nan), '')]]
            data.append(dict(zip(data_cols, [range_name, values])))

    for item in write_chunks(data=data, size=3000):
        body = {'valueInputOption': 'RAW', 'data':item}
        response = sheet.values().batchUpdate(
            spreadsheetId=sheet_id, body=body).execute()

        if not 'totalUpdatedRows' in response.keys():
            return False

    return True
