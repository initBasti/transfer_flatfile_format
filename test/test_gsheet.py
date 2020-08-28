import pytest

from transfer_flatfile_format.packages.google_sheet import (
    build_column_name
)

def test_build_column_name():
    expect = ['A', 'D', 'AB', 'BF', 'ZZ']
    result = []

    sample_input = [0, 3, 27, 57, 701]

    for i in sample_input:
        result.append(build_column_name(column_enum=i))

    assert expect == result
