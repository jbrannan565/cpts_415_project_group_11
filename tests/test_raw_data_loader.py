from code_snippets.raw_data_loader import *

def test_boundary_data():
    # test row number
    assert len(boundary_raw_df.index) == 50
    # test column number
    assert len(boundary_raw_df.columns) == 2

def test_airlines_data():
    # test row number
    assert len(airlines_raw_df.index) == 6162
    # test column number
    assert len(airlines_raw_df.columns) == 8

def test_airports_data():
    # test row number
    assert len(airports_raw_df.index) == 7698
    # test column number
    assert len(airports_raw_df.columns) == 14

def test_routes_data():
    # test row number
    assert len(routes_raw_df.index) == 67663
    # test column number
    assert len(routes_raw_df.columns) == 9
