import numpy as np
import pandas as pd
import os
import tqdm
import pickle
import json
from datetime import datetime

import argparse


def batch_dirs(root, batchsize=25):
    """Returns a list of n lists
    """
    week_dirs = os.listdir(root)
    week_dirs.sort()
    batch_list = []
    for i in range(0, len(week_dirs), batchsize):
        batch = week_dirs[i:i+batchsize]
        batch_list.append(batch)
    return batch_list


def transform_dataframe(dirs, root, out_dir):
    """Unpacks safegraph json into pandas dataframe
    """
    bad_files = []
    weekly_df = pd.DataFrame()

    for dir in tqdm.tqdm(dirs, desc="directory", position=0):
        start, end = str.split(dir, 'through')
        start = datetime.strptime(start, '%Y-%m-%d')
        end = datetime.strptime(end, '%Y-%m-%d')

        files = os.listdir(f"{root}/{dir}")
        for file in files:
            f = open(f"{root}/{dir}/{file}")
            try:
                a = json.load(f)
                df = pd.json_normalize(a['data']['search']['places']['results']['edges'])
            except:
                print(file)
                bad_files.append(file)
                continue

        def getValues(arr):
            if arr is not None:
                return list(arr.values())
            else:
                return [np.nan for i in range(7)]

        # get visits by day of week
        sub_df = df.loc[:,
                        ['node.placekey', 'node.safegraph_core.naics_code',
                         'node.safegraph_core.location_name',
                         'node.weekly_patterns',
                         'node.safegraph_core.top_category',
                         'node.safegraph_core.sub_category',
                         'node.safegraph_geometry.wkt_area_sq_meters']]
        for i in range(7):
            visits = sub_df['node.weekly_patterns'].str[0].str['visits_by_day'].str[i].str['visits']
            sub_df.loc[:, f'day_{i}_visits'] = visits
        raw_visitor_counts = sub_df['node.weekly_patterns'].str[0].str['raw_visitor_counts']
        raw_visit_counts = sub_df['node.weekly_patterns'].str[0].str['raw_visit_counts']
        sub_df.loc[:, 'raw_visitor_counts'] = raw_visitor_counts
        sub_df.loc[:, 'raw_visit_counts'] = raw_visit_counts

        # get median dwell
        median_dwell = sub_df['node.weekly_patterns'].str[0].str['median_dwell']
        sub_df['median_dwell'] = median_dwell
        buckets = ['<5', '5-10', '11-20', '21-60', '61-120', '121-240', '>240']
        dwell_bucket_values = sub_df['node.weekly_patterns'].str[0].str['bucketed_dwell_times'].apply(getValues)
        v = dwell_bucket_values.values.tolist()
        sub_df.loc[:, buckets] = pd.DataFrame(v, sub_df.index, buckets)

        sub_df.drop(columns=['node.weekly_patterns'], inplace=True)

        weekly_df = pd.concat([weekly_df, sub_df])

    filename_pre = f"{dirs[0]}_{dirs[-1]}"
    path = f"{out_dir}/{filename_pre}.csv"
    weekly_df.to_csv(path)

    with open(f'{out_dir}/{filename_pre}_bad_files.pickle', 'wb') as handle:
        pickle.dump(bad_files, handle, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--root", type=str, default='G://My Drive/Research/Safegraph/data/')
    parser.add_argument(
        "--out_dir", type=str, default='G://My Drive/Research/Safegraph/scratch/20221205/')
    args = parser.parse_args()

    batch_list = batch_dirs(args.root)
    print([b[0] + b[-1] for b in batch_list])
    transform_dataframe(batch_list[8], args.root, args.out_dir)
