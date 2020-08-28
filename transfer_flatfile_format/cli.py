"""
    Author: Sebastian Fricke, Panasiam
    Date: 2020-08-28
    License: GPLv3

    Move data inbetween different flatfile formats to the correct postion.
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
            path [String]   - Path string from argument parser

        Return:
            [String]    - Return the full valid path
    """
    full_path = os.path.join(os.getcwd(), path)
    if not os.path.exists(full_path):
        if os.path.exists(path):
            full_path = path
        else:
            print(f"Path [{full_path}] not valid")
            return ''
    return full_path

def create_match_table(sheet, intern_list):
    """
        Create a data-frame of the SKUs found in the google sheet,
        together with an alternative to improve the chances of finding
        a match.

        Parameter:
            sheet [DataFrame]       - The google sheet data
            intern_list [DataFrame] - A separate list with alternative SKUs

        Return:
            [DataFrame] - SKU/Alternative SKU
    """
    reduced_intern =\
        intern_list[['Variation.number', 'Variation.externalId']].rename(
            columns={'Variation.number':'item_sku',
                     'Variation.externalId':'alt_sku'})
    reduced_intern = reduced_intern.astype({'item_sku':object})
    merge = sheet.merge(reduced_intern, how='left')
    merge['alt_sku'].replace(np.nan, '', inplace=True)

    return merge[['item_sku', 'alt_sku']]

def find_match(sku, header, source, table):
    """
        Check if the given SKU or the alternative SKU (from the match table)
        can be located in the orignal format (SOURCE). Try to fetch the value
        from the column HEADER if that one is present in the source.

        Parameter:
            sku [String]        - given SKU from the google sheet
            header [String]     - Name of the column from the google sheet
            source [DataFrame]  - original Flatfile format from CLI
            table  [DataFrame]  - Table with google sheet SKUs and matching
                                  alternative SKUs

        Return:
            [String/Int]        - Value from the combination of SKU/HEADER
                                  OR ''
    """
    if header not in source.columns:
        return ''

    source_match = source[source['item_sku'] == sku]
    table_match = table[table['item_sku'] == sku]

    if len(source_match.index) > 0:
        try:
            value = source_match[header]
        except KeyError:
            return ''
    elif len(table_match.index) == 0:
        return ''
    elif len(source[source['item_sku'] == table_match['alt_sku'].values[0]].index) > 0:
        try:
            value = source[source['item_sku'] == table_match['alt_sku'].values[0]][header]
        except KeyError:
            return ''
    else:
        return ''
    if len(value.index) == 0:
        return ''
    if value.values[0] == 0:
        return ''
    return value.values[0]

def transfer_from_original(gsheet, source, match_table):
    """
        Fill out columns, that can be located in the google sheet as well as
        the original flatfile format, within the google sheet with values from
        the source flatfile.

        Parameter:
            gsheet [DataFrame]      - Google sheet containing the target
                                      flatfile format
            source [DataFrame]      - Source flatfile format from the CLI
            match_table [DataFrame] - Frame containing a SKU/Altenative Sku
                                      mapping

        Return:
            [DataFrame]         - Google sheet with filled out values from the
                                  original format
    """
    for header in gsheet.columns:
        if header in ['item_sku', 'index']:
            continue
        gsheet[header] = gsheet['item_sku'].apply(
            lambda x: find_match(sku=x, header=header, source=source,
                                 table=match_table))
        # filter remaining '0' values
        if gsheet[header].dtypes == object:
            if len(gsheet[gsheet[header].str.contains(r"^0$", na=False)].index) > 0:
                gsheet[header] = gsheet[header].str.replace(r"^0$", '')
    return gsheet

def cli():
    orig_path = ''
    sheet_id = ''

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--original', required=True,
                        help='The original flatfile format',
                        action='store', dest='original')
    parser.add_argument('-c', '--column', required=False,
                        help='choose a specific column from the data source',
                        action='store', dest='column')
    args = parser.parse_args()

    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    sheet_id = config['General']['google_sheet_id']

    creds = google_sheet.get_google_credentials()

    orig_path = check_path(path=args.original)

    if not orig_path:
        print("path to required file not valid\n[{0}]".format(orig_path))
        sys.exit(1)

    # Parse the EAN field explicitly to a string to read EANs starting with
    # '0..' correctly, do the same for the browse nodes to avoid floating
    # points
    orig = pandas.read_csv(orig_path, sep=';',
                           dtype={'external_product_id':object,
                                  'recommended_browse_nodes':object})

    print("Downloading alternative SKUs..")
    inter = pandas.read_csv(config['General']['sku_export'], sep=';')
    print("finished.")

    if args.column:
        gsheet = google_sheet.read_specified_column(creds=creds,
                                                    sheet_id=sheet_id,
                                                    target_column=args.column)
    elif not args.column:
        gsheet = google_sheet.read_incomplete_data(creds=creds,
                                                   sheet_id=sheet_id)

    if len(gsheet.index) == 0:
        sys.exit(1)

    match_table = create_match_table(sheet=gsheet, intern_list=inter)

    if args.column:
        gsheet['value'] = gsheet['item_sku'].apply(
            lambda x: find_match(sku=x, header=args.column, source=orig,
                                 table=match_table))
    elif not args.column:
        gsheet = transfer_from_original(gsheet=gsheet, source=orig,
                                        match_table=match_table)

    gsheet.to_csv(os.path.join('/', 'home', 'basti', 'test_transfer.csv'), sep=';',
                  index=False)

    google_sheet.write_google_sheet(creds=creds, sheet_id=sheet_id,
                                    frame=gsheet)
