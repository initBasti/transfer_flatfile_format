# Transfer flatfile format

## Overview

#### Purpose:

The underlying problem, which the project tackles is that Amazon provides different flatfile formats. Either because the user chooses different elements or because amazon applies changes to an existing format. In both cases, it is impossible to simply copy and paste data in between formats, when the column headers don't match.

#### Strategy:

Find a sub-set of rows, that match a certain condition (don't contain any values, besides the provided SKU), pull the missing data from an external source supplied through the command line option (-o/--original). Map a fallback value for the SKU from the google sheet by searching for one inside of a plentymarkets export (this is a very specific option usable for our system). Write the data to the google-sheet in form of smaller chunks.
