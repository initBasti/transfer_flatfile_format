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
import argparse
import configparser
import pandas
import numpy as np

from transfer_flatfile_format.packages import google_sheet

USER = os.getlogin()
if sys.platform == 'linux':
    DATA_DIR = os.path.join('/', 'home', str(f'{USER}'),
                            '.transfer_flatfile_format_data/')
elif sys.platform == 'win32':
    DATA_DIR = os.path.join('C:\\', 'Users', str(f'{USER}'),
                            '.transfer_flatfile_format_data/')
CONFIG_PATH = os.path.join(DATA_DIR, 'config.ini')


def check_path(path):
    """
        Check if the path supplied through the command line interface is
        a subpart of the actual path / a full path or invalid.

        Parameter:
            path [String]   -   Path string from argument parser

        Return:
            [String]        -   Return the full valid path
    """
    full_path = os.path.join(os.getcwd(), path)
    if not os.path.exists(full_path):
        if os.path.exists(path):
            full_path = path
        else:
            print(f"Path [{full_path}] not valid")
            return ''
    return full_path


def get_matchtable_data(config):
    data = {'activate': False, 'main_sku': '', 'alt_sku': '', 'src': ''}

    if not config:
        return data

    if 'Match_table' not in config.sections():
        return data

    if (not config.has_option(section='Match_table', option='main_sku') or\
            not config.has_option(section='Match_table', option='alt_sku') or\
            not config.has_option(section='Match_table', option='sku_export') or\
            not config.has_option(section='Match_table', option='with_matchtable')):
        print("WARNING: Match table configuration data not complete")
        return data

    if config['Match_table']['with_matchtable'].lower()[0] == 'y':
        data['activate'] = True
    else:
        return data

    data['main_sku'] = config['Match_table']['main_sku']
    data['alt_sku'] = config['Match_table']['alt_sku']
    data['src'] = config['Match_table']['sku_export']

    return data


def get_exclude_options(string):
    """
        Parse the command line option to check if the columns are valid
        and separated in the correct format.

        Parameter:
            string [String]     -   single column or multiple separated columns

        Return:
            [List]
    """
    if string.find(','):
        return string.strip(' ').split(',')
    return [string.strip(' ')]


def exclude_columns(data, columns):
    """
        Check if the columns from the command line option 'exclude'
        match the columns read from the google sheet.

        Parameter:
            data [DataFrame]    -   Data from the google sheet
            columns [String]    -   Columns for exclusion from CLI

        Return:
            [List]              -   List of valid columns to exclude
    """
    if not columns:
        return []

    sub_arguments = get_exclude_options(string=columns)

    for arg in sub_arguments:
        if arg not in data.columns:
            print(f"WARNING: No column named [{arg}] in the google sheet")
            sub_arguments.remove(arg)

    return sub_arguments


def create_match_table(sheet, intern_list, config):
    """
        Create a data-frame of the SKUs found in the google sheet,
        together with an alternative to improve the chances of finding
        a match.

        Parameter:
            sheet [DataFrame]       -   The google sheet data
            intern_list [DataFrame] -   A separate list with alternative SKUs
            config [Dict]           -   match table information from the config

        Return:
            [DataFrame] - SKU/Alternative SKU
    """
    main_sku = config['main_sku']
    alt_sku = config['alt_sku']

    reduced_intern =\
        intern_list[[main_sku, alt_sku]].rename(
            columns={main_sku:'item_sku',
                     alt_sku:'alt_sku'})
    reduced_intern = reduced_intern.astype({'item_sku': object})
    merge = sheet.merge(reduced_intern, how='left')
    merge['alt_sku'].replace(np.nan, '', inplace=True)

    return merge[['item_sku', 'alt_sku']]


def adjust_value(data, config):
    """
        Use a simple python expression to modify all values from the
        original flatfile.

        Parameter:
            data [DataFrame]    -   GoogleSheet
            config [ConfigParser object]
    """
    # remove NaN values from the sheet
    data['value'].fillna(0, inplace=True)
    data['value'] = data['value'].astype(str)
    if config.has_option(section='Adjust', option='command'):
        data['value'] = data['value'].apply(lambda x: eval(
            config['Adjust']['command'].replace('X', str(x))
        ))
        data['value'] = data['value'].astype(str)
        data['value'] = data['value'].str.replace('^0$', '')
    else:
        print("ERROR: Add a 'command' option to the config for '-a'")
        sys.exit(1)


def find_match(sku, header, source, table):
    """
        Check if the given SKU or the alternative SKU (from the match table)
        can be located in the orignal format (SOURCE). Try to fetch the value
        from the column HEADER if that one is present in the source.

        Parameter:
            sku [String]        -   given SKU from the google sheet
            header [String]     -   Name of the column from the google sheet
            source [DataFrame]  -   original Flatfile format from CLI
            table  [DataFrame]  -   Table with google sheet SKUs and matching
                                    alternative SKUs

        Return:
            [String/Int]        -   Value from the combination of SKU/HEADER
                                    OR ''
    """
    if header not in source.columns:
        return ''

    source_match = source[source['item_sku'] == sku]
    try:
        table_match = table[table['item_sku'] == sku]
    except KeyError:
        table_match = pandas.DataFrame()

    if len(source_match.index) > 0:
        value = source_match[header]
    elif len(table_match.index) == 0:
        return ''
    elif len(source[source['item_sku'] ==
                    table_match['alt_sku'].values[0]].index) > 0:
        value = source[source['item_sku'] ==
                       table_match['alt_sku'].values[0]][header]
    else:
        return ''
    if len(value.index) == 0:
        return ''
    if value.values[0] == 0:
        return ''
    return value.values[0]


def transfer_from_original(gsheet, source, match_table, exclude):
    """
        Fill out columns, that can be located in the google sheet as well as
        the original flatfile format, within the google sheet with values from
        the source flatfile.

        Parameter:
            gsheet [DataFrame]      -   Google sheet containing the target
                                        flatfile format
            source [DataFrame]      -   Source flatfile format from the CLI
            match_table [DataFrame] -   Frame containing a SKU/Altenative Sku
                                        mapping
            exclude[List]           -   List of columns to exclude

        Return:
            [DataFrame]             -   Google sheet with filled out values
                                        from the original format
    """
    for header in gsheet.columns:
        if header in ['item_sku', 'index']:
            continue
        if header in exclude:
            continue
        gsheet[header] = gsheet['item_sku'].apply(lambda x: find_match(
            sku=x, header=header, source=source, table=match_table))
        # filter remaining '0' values
        if gsheet[header].dtypes == object:
            if len(gsheet[gsheet[header].str.contains(r"^0$",
                                                      na=False)].index) > 0:
                gsheet[header] = gsheet[header].str.replace(r"^0$", '')
    return gsheet


def set_up_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-o',
        '--original',
        required=True,
        action='store',
        dest='original',
        help='original flatfile format')
    parser.add_argument(
        '-c',
        '--column',
        required=False,
        action='store',
        dest='column',
        help='choose a specific column from the data source')
    parser.add_argument(
        '-e',
        '--exclude',
        required=False,
        action='store',
        dest='exclude',
        help='exclude a column/list of column names from being overwritten')
    parser.add_argument(
        '-s',
        '--save',
        required=False,
        action='store_true',
        dest='save',
        help='save the changes into a file at ~/.transfer_flatfile_format')
    parser.add_argument(
        '-a',
        '--adjust_value',
        required=False,
        action='store_true',
        dest='adjust',
        help='Only with --column, use a command from config to adjust values')
    args = parser.parse_args()

    if args.adjust and not args.column:
        print("ERROR: You can only use --adjust in combination with --column")
        sys.exit(1)

    return args


def cli():
    orig_path = ''
    sheet_id = ''
    ex = []
    match_table = pandas.DataFrame()
    with_matchtable = False

    args = set_up_argparser()

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    sheet_id = config['General']['google_sheet_id']
    matchtable_data = get_matchtable_data(config=config)

    if matchtable_data['activate']:
        with_matchtable = True

    creds = google_sheet.get_google_credentials()

    orig_path = check_path(path=args.original)

    if not orig_path:
        print("path to required file not valid\n[{0}]".format(orig_path))
        sys.exit(1)

    # We just want to copy values so just take everything as a string
    orig = pandas.read_csv(orig_path, sep=';', dtype=str)
    if 'item_sku' not in orig.columns:
        orig = pandas.read_csv(orig_path, sep=';', dtype=str, header=2)
        if 'item_sku' not in orig.columns:
            print("ERROR: invalid flatfile from '--original'")
            print("\tCould not locate 'item_sku' in row 1 or 3")
            sys.exit(1)

    if len(orig.index) == 0:
        print(f"ERROR: Empty file provides by '--original' @ {orig_path}")


    if args.column:
        gsheet = google_sheet.read_specified_column(creds=creds,
                                                    sheet_id=sheet_id,
                                                    target_column=args.column)
    elif not args.column:
        gsheet = google_sheet.read_incomplete_data(creds=creds,
                                                   sheet_id=sheet_id)

    if len(gsheet.index) == 0:
        sys.exit(1)

    if args.exclude:
        ex = exclude_columns(data=gsheet, columns=args.exclude)
        if not ex:
            print("ERROR: Option '-e' needs a ',' separated list of strings")
            sys.exit(1)

    if with_matchtable:
        print("Downloading alternative SKUs..")
        inter = pandas.read_csv(matchtable_data['src'], sep=';')
        print("finished.")
        match_table = create_match_table(sheet=gsheet,
                                         intern_list=inter,
                                         config=matchtable_data)

    if args.column:
        gsheet['value'] = gsheet['item_sku'].apply(lambda x: find_match(
            sku=x, header=args.column, source=orig, table=match_table))
        if args.adjust and config.has_section('Adjust'):
            adjust_value(data=gsheet, config=config)
        elif args.adjust and not config.has_section('Adjust'):
            print("ERROR: Add a 'Adjust' section and 'command' option for -a")
            sys.exit(1)
        gsheet = gsheet[gsheet['value'] != '']
    elif not args.column:
        gsheet = transfer_from_original(gsheet=gsheet,
                                        source=orig,
                                        match_table=match_table,
                                        exclude=ex)

    if args.save:
        gsheet.to_csv(os.path.join(DATA_DIR, 'last_changes.csv'),
                      sep=';',
                      index=False)

    google_sheet.write_google_sheet(creds=creds,
                                    sheet_id=sheet_id,
                                    frame=gsheet,
                                    exclude=ex)
