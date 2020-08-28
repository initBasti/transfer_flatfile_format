import pytest
import pandas
import numpy as np
from pandas.testing import assert_frame_equal

from transfer_flatfile_format.cli import (
    create_match_table, find_match
)

@pytest.fixture
def sample_google_sheet():
    llist = [
        ['1234x', 'abc', '', '', ''],
        ['1235x', 'abc1', '', '', ''],
        ['1236x', 'abc2', '', '', ''],
        ['1237x', 'abc3', '', '', ''],
        ['1238x', 'abc4', '', '', ''],
        ['1239x', 'abc5', '', '', ''],
        ['1240x', 'abc6', '', '', ''],
        ['1241x', 'abc7', '', '', ''],
        ['1243x', 'abc8', '', '', '']
    ]

    frame = pandas.DataFrame(llist, columns=['item_sku', 'test', 'test2',
                                             'test3', 'test4'])
    return frame

@pytest.fixture
def sample_intern_list():
    llist = [
        ['1234x', '2345x'],
        ['1235x', '2346x'],
        ['1236x', '2347x'],
        ['1237x', '2348x'],
        ['1238x', '2349x'],
        ['1239x', '2350x'],
        ['1240x', '2351x'],
        ['1242x', '2353x'],
        ['1243x', '']
    ]

    frame = pandas.DataFrame(llist, columns=['Variation.number',
                                             'Variation.externalId'])
    return frame

@pytest.fixture
def sample_google_sheet_small():
    llist = [
        ['1234x', 'abc', '', '', ''],
        ['1235x', 'abc1', '', '', ''],
        ['1236x', 'abc2', '', '', '']
    ]

    frame = pandas.DataFrame(llist, columns=['item_sku', 'test', 'test2',
                                             'test3', 'test4'])
    return frame

@pytest.fixture
def sample_original_format():
    llist = [
        ['1234x', 'abc', 'a', 'b'], # Correct
        ['wrong_sku', 'abc1', 'd', 'e'], # Wrong SKU
        ['2347x', 'abc2', 'g', 'h'], # Alternative SKU
    ]

    frame = pandas.DataFrame(llist, columns=['item_sku', 'test', 'test2',
                                             'test3'])
    return frame

@pytest.fixture
def sample_match_table():
    llist = [
        ['1234x', '2345x'],
        ['1235x', '2346x'],
        ['1236x', '2347x']
    ]

    frame = pandas.DataFrame(llist, columns=['item_sku', 'alt_sku'])
    return frame

def test_create_match_table(sample_google_sheet, sample_intern_list):
    expect = [
        ['1234x', '2345x'], ['1235x', '2346x'], ['1236x', '2347x'],
        ['1237x', '2348x'], ['1238x', '2349x'], ['1239x', '2350x'],
        ['1240x', '2351x'], ['1241x', ''], ['1243x', '']
    ]
    expect = pandas.DataFrame(expect, columns=['item_sku', 'alt_sku'])

    result = create_match_table(sheet=sample_google_sheet,
                                intern_list=sample_intern_list)

    assert_frame_equal(expect, result)

def test_find_match(sample_google_sheet_small, sample_original_format,
                    sample_match_table):
    expect = ['a', 'b', '', '', '', '', 'g', 'h', '']
    result = []

    iterator = sample_google_sheet_small.iterrows()

    for row in iterator:
        for header in ['test2', 'test3', 'test4']:
            result.append(find_match(row[1].item_sku, header,
                                     sample_original_format,
                                     sample_match_table))

    assert expect == result
