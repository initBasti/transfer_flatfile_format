# Transfer flatfile format

## Overview

#### Purpose:

The underlying problem, which the project tackles is that Amazon provides different flatfile formats. Either because the user chooses different elements or because amazon applies changes to an existing format. In both cases, it is impossible to simply copy and paste data in between formats, when the column headers don't match.

#### Strategy:

Find a sub-set of rows, that match a certain condition (don't contain any values, besides the provided SKU), pull the missing data from an external source supplied through the command line option (-o/--original). Map a fallback value for the SKU from the google sheet by searching for one inside of a plentymarkets export (this is a very specific option usable for our system). Write the data to the google-sheet in form of smaller chunks.

#### Installation:

`python3 -m pip install transfer_flatfile_format --user --upgrade`

#### Usage:

There are **three* options:

- --orginal / -o:
    + File location of the flatfile format, which is used as source for the values
- --exclude / -e:
    + Comma-separated list of column names (3rd row of a flatfile), to exclude from writing to the google sheet (use case: some columns from the source contain outdated values)
- --column / -c:
    + A column name (3rd row of a flatfile), to exclusivly transfer from the source to the google sheet

Additionally, there is the `config.ini` file within:
- ~/.transfer_flatfile_format/config.ini (on Linux)
- C:\\Users\{USER}\.transfer_flatfile_format (on Windows)

Which is used to specify the ID of the google sheet and optionally a data source for alternative SKUs.
The alternative SKU can be used if your system maintains more than one SKU for one entity. That way you can match a product with one of two possible terms.

Example:

config.ini

```
[General]
google_sheet_id=1PB_XrUqy6qk......
[Match table]
with_matchtable={y|n}
sku_export={Link to csv file or location in file system}
main_sku={column_name of the column where the main SKU is located}
alt_sku={column_name of the column where the alternative SKU is located}
```

##### Example 1: Upload all values from the source file to the google sheet, when the google sheet has an SKU but no values in 'brand_name' or 'item_name':

`python3 -m transfer_flatfile_format -o /home/path/to/source_file.csv`

##### Example 2: Upload all values from the source file at column 'example_column' to the google sheet:

`python3 -m transfer_flatfile_format -o /home/path/to/source_file.csv -c example_column`

##### Example 3: Do the same as with 'Example 1' but do not update the columns 'example_col1' & 'example_col2':

`python3 -m transfer_flatfile_format -o /home/path/to/source_file.csv -e example_col1,example_col2`
