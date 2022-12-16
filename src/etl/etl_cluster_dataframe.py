import numpy as np
import pandas as pd
import glob
import pickle
from datetime import datetime
from collections import Counter

import argparse


def print_bad_files(in_dir):
    """Combines pickle files of lists of json files that didn't load
    """
    pickle_files = glob.glob(f'{in_dir}/*.pickle')
    bad_files = []
    for picklepath in pickle_files:
        with open(picklepath, 'rb') as f:
            bad_files.append(pickle.load(f))
    bad_files = [item for sublist in bad_files for item in sublist]
    print(bad_files)


def combine_batch_csv(in_dir):
    """Combines Safegraph dataframes into one
    """
    csv_files = glob.glob(f'{in_dir}/*.csv')

    df_list = []

    for filename in csv_files:
        df = pd.read_csv(filename, header=0, index_col=0)
        df_list.append(df)

    df = pd.concat(df_list, axis=0)

    return df


def clean_df(df, density_cutoff=150, dwell_cutoff=500, avg_by_naics=True):
    """ Returns dataframe with density and median dwell time info
    """
    # drop nans
    df = df[~df['raw_visitor_counts'].isnull()]

    # set types
    df['node.safegraph_core.naics_code'] = df['node.safegraph_core.naics_code'].astype(int).astype(str)
    naics_code_lengths = df['node.safegraph_core.naics_code'].apply(len).tolist()
    print(Counter(naics_code_lengths))

    # create features
    df['density'] = df['raw_visit_counts'] / df['node.safegraph_geometry.wkt_area_sq_meters']

    # remove outliers
    df = df.loc[(df['median_dwell'] < dwell_cutoff) & (df['density'] < density_cutoff),:]

    # filter to 2019
    df['start'] = pd.to_datetime(df.start, format='%Y-%m-%d')
    df['end'] = pd.to_datetime(df.end, format='%Y-%m-%d')
    df = df.loc[(df.start > '2019-01-01') & (df.start < '2019-12-31'), :]

    # log transform
    df['log_density'] = np.log(df['density'])
    df['log_median_dwell'] = np.log(df['median_dwell'])

    # aggregate
    if avg_by_naics:
        df_mean = df.groupby('node.safegraph_core.naics_code')['log_density', 'log_median_dwell'].mean()
    else:
        df_mean = df.groupby('node.placekey')['log_density', 'log_median_dwell'].mean()
   
    df_mean = df_mean.reset_index()

    return df_mean



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--in_dir", type=str, default='G:/My Drive/Research/Safegraph/scratch/20221205/')
    parser.add_argument(
        "--out_dir", type=str, default='G:/My Drive/Research/Safegraph/scratch/20221205/')
    args = parser.parse_args()

    df = combine_batch_csv(args.in_dir)
    df_mean = clean_df(df)
    df_mean.to_csv(f"{args.out_dir}/df_2019_naics.csv", index=False)
