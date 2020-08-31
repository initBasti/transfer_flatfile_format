"""
    transfer_flatfile_format
    Move data inbetween different flatfile formats to the correct postion.
    Copyright (C) 2020  Sebastian Fricke, Panasiam

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import sys
import os
import math
import pickle
from itertools import islice
import pandas
import numpy as np

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

LEN_ALPHABET = 26
SKU_COLUMN = 1
BRAND_COLUMN = 2
NAME_COLUMN = 5
HEADER_ROW = 3

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
            column [String]         -   Start column on the sheet ('A' etc.)
            max_row [Integer]       -   Amount of rows for the range
            range_column [String]   -   End column on the sheet
                                        (To create a horizontal-vertical
                                         range (A1:E2))

        Return:
            [String]                -   Example: 'A1:E5' (A1, A2, ... , E4, E5)
    """
    if max_row:
        if not range_column:
            return str(f'{column}1:{column}{max_row}')
        return str(f'{column}1:{range_column}{max_row}')
    print("No specified amount of rows to read, default to range: {0}".format(
        'A1:E20'))
    print("Check the config.ini file to adjust the range")
    return 'A1:E20'


def fill_up_values(val, maximum):
    """
        Append empty values to a list for the specified range.

        Parameter:
            val [List]      -   original list
            maximum [Int]   -   End of range

        Return:
            val [List]
    """
    for _ in range(len(val), maximum):
        val.append('')
    return val


def build_column_name(column_enum):
    """
        Parse a letter combination from a give 0-indexed column index number.
        example: 27 => 'AB', 3 => 'D', 0 => 'A'

        Parameter:
            column_enum [Int]   -   index number of the column

        Return:
            [String]
    """
    a_const = ord('A')
    quotient = math.floor(column_enum / LEN_ALPHABET)

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
            data [List]     -   Data containing dictionaries with ranges and
                                values for a write
            size [int]      -   chunk size

        Return:
            [List]          -   Sub-list of size SIZE
    """
    iterator = iter(data)
    for _ in range(0, len(data), size):
        yield [k for k in islice(iterator, size)]


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
        except PermissionError:
            print("Initial setup needs root permissions (sudo ..)")

    return creds


def read_google_sheet(creds, sheet_id):
    """
        Open the sheet with the @sheet_id.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String]       -   Identification of the google sheet

        Return:
            [Dict] : Response from google sheets API
    """
    service = build('sheets', 'v4', credentials=creds)
    sheet_range = [
        build_sheet_range(column='A', max_row='2400', range_column='FH')
    ]

    sheet = service.spreadsheets()
    result = sheet.values().batchGet(spreadsheetId=sheet_id,
                                     ranges=sheet_range).execute()
    ranges = result['valueRanges']
    if not ranges:
        print('No data found')

    return ranges


def read_incomplete_data(creds, sheet_id):
    """
        Read only rows from the google sheet, that match the following pattern:
            - 'item_sku' field is filled
            - 'brand_name' & 'item_name' field are not filled => empty values
        Save the data into a dataframe, with all possible columns from the
        google sheet source, even if the values are empty.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String]       -   Identification of the google sheet

        Result:
            [DataFrame]

    """
    sheet_dict = {}

    ranges = read_google_sheet(creds=creds, sheet_id=sheet_id)
    if not ranges:
        return pandas.DataFrame()

    for data in ranges:
        values = data['values']
        column_names = values[2]
        for i in range(HEADER_ROW, len(values)):
            if not values[i][SKU_COLUMN]:
                continue
            # Only take rows which are not filled out
            if len(values[i]) > 2:
                if values[i][BRAND_COLUMN] and values[i][NAME_COLUMN]:
                    continue

            if len(values[i]) < len(column_names):
                values[i] = fill_up_values(val=values[i],
                                           maximum=len(column_names))
            for index, column in enumerate(column_names):
                if column not in sheet_dict.keys():
                    sheet_dict[column] = []
                if not values[i][index]:
                    sheet_dict[column].append('')
                else:
                    sheet_dict[column].append(values[i][index])
            if 'index' not in sheet_dict.keys():
                sheet_dict['index'] = []
            sheet_dict['index'].append(i)

    return pandas.DataFrame(sheet_dict, columns=column_names + ['index'])


def read_specified_column(creds, sheet_id, target_column):
    """
        Read every SKU together with the specified column into dataframe.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String]       -   Identification of the google sheet
            target_column [String]  -   command line argument specifying the
                                        target column

        Result:
            [DataFrame]

    """
    sheet_dict = {}

    ranges = read_google_sheet(creds=creds, sheet_id=sheet_id)
    if not ranges:
        return pandas.DataFrame()

    for data in ranges:
        values = data['values']
        column_names = values[2]
        if not target_column in column_names:
            print(f"ERROR: column {target_column} not found @ google sheet.")
            return pandas.DataFrame()

        sheet_dict = {
            col: [] for col in ['item_sku', 'value', 'column_index', 'index']
        }
        for i in range(HEADER_ROW, len(values)):
            if not values[i][SKU_COLUMN]:
                continue
            sheet_dict['item_sku'].append(values[i][SKU_COLUMN])
            if len(values[i]) < len(column_names):
                values[i] = fill_up_values(val=values[i],
                                           maximum=len(column_names))

            for index, column in enumerate(column_names):
                if column == target_column:
                    if not values[i][index]:
                        sheet_dict['value'].append('')
                    else:
                        sheet_dict['value'].append(values[i][index])
                    sheet_dict['column_index'].append(index)
                    sheet_dict['index'].append(i)

    return pandas.DataFrame(
        sheet_dict, columns=['item_sku', 'value', 'index', 'column_index'])


def write_google_sheet(creds, sheet_id, frame, exclude):
    """
        Write the values to the google sheet, depending on the read option
        a 'column_index' column is present (when the column option was used),
        in that case only write that specific column.

        Parameter:
            creds [Google Sheet credentials]
            sheet_id [String]   -   Identification of the google sheet
            frame [DataFrame]   -   difference between source and target
            exclude [List]      -   columns to exclude from writing to gsheet
    """
    data = []
    data_cols = ['range', 'values']

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    generator = frame.iterrows()
    for item in generator:
        if 'column_index' in item[1].keys():
            range_name = build_column_name(item[1]['column_index'])\
                + str(item[1]['index']+1)
            values = [[str(item[1]['value']).replace(str(np.nan), '')]]
            data.append(dict(zip(data_cols, [range_name, values])))
            continue
        for i, col in enumerate([x for x in frame.columns if x != 'index']):
            if col == 'item_sku':
                continue
            if col in exclude:
                continue
            range_name = build_column_name(i) + str(item[1]['index'] + 1)
            values = [[str(item[1][col]).replace(str(np.nan), '')]]
            data.append(dict(zip(data_cols, [range_name, values])))

    for item in write_chunks(data=data, size=3000):
        body = {'valueInputOption': 'RAW', 'data': item}
        response = sheet.values().batchUpdate(spreadsheetId=sheet_id,
                                              body=body).execute()

        if not 'totalUpdatedRows' in response.keys():
            print("WARNING: No updates were performed.")
