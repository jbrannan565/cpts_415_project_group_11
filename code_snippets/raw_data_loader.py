"""
Description: load in the files from raw_data. Creates the following variables as pandas dataframes:
    - boundary_raw_df
    - airlines_raw_df
    - airports_raw_df
    - routes_raw_df
"""
import pandas as pd

def load_boundary_data():
    return pd.read_csv("raw_data/boundary-each-state.tsv", sep="\t", header=None)

def load_airlines_data():
    return pd.read_csv("raw_data/airlines.dat", header=None)

def load_airports_data():
    return pd.read_csv("raw_data/airports.dat", header=None)

def load_routes_data():
    return pd.read_csv("raw_data/routes.dat", header=None)

boundary_raw_df = load_boundary_data()
airlines_raw_df = load_airlines_data()
airports_raw_df = load_airports_data()
routes_raw_df   = load_routes_data()
