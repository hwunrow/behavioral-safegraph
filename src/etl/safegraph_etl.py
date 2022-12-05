import numpy as np
import pandas as pd
import os
import json
import tqdm
from collections import Counter

def extract_from_json(file):
    j = json.load(file)
    df = pd.json_normalize(a['data']['search']['places']['results']['edges'])
    return(df)


def parse_visits(sub_df):
    for i in range(7):
        visits = sub_df['node.weekly_patterns'].str[0].str['visits_by_day'].str[i].str['visits']
        sub_df.loc[:,f'day_{i}_visits'] = visits
    raw_visitor_counts = sub_df['node.weekly_patterns'].str[0].str['raw_visitor_counts']
    raw_visit_counts = sub_df['node.weekly_patterns'].str[0].str['raw_visit_counts']
    sub_df.loc[:,'raw_visitor_counts'] = raw_visitor_counts
    sub_df.loc[:,'raw_visit_counts'] = raw_visit_counts
    return(sub_df)


def parse_dwell(sub_df):
    median_dwell = sub_df['node.weekly_patterns'].str[0].str['median_dwell']
    sub_df['median_dwell'] = median_dwell
    buckets = ['<5', '5-10', '11-20', '21-60', '61-120', '121-240', '>240']
    dwell_bucket_values = sub_df['node.weekly_patterns'].str[0].str['bucketed_dwell_times'].apply(getValues)
    v = dwell_bucket_values.values.tolist()
    sub_df.loc[:,buckets] = pd.DataFrame(v, sub_df.index, buckets)
    return(sub_df)

def transform_dataframe(in_dirs, out_dir):
  bad_files = []
  weekly_df = pd.DataFrame()

  for dir in tqdm.tqdm(in_dirs, desc="directory", position=0):
    files = os.listdir(f"{root}/{dir}")
    for file in files:
      f = open(f"{root}/{dir}/{file}")
      try:
        df = extract_from_json(f)
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
      mycols = ['node.placekey','node.safegraph_core.naics_code','node.weekly_patterns', 'node.safegraph_core.top_category', 'node.safegraph_core.sub_category', 'node.safegraph_geometry.wkt_area_sq_meters']
      sub_df = df.loc[:,mycols]
      sub_df = parse_visits(sub_df)

      # get median dwell
      sub_df = parse_dwell(sub_df)

      sub_df.drop(columns=['node.weekly_patterns'], inplace=True)

      weekly_df = pd.concat([weekly_df, sub_df])
  
  filename_pre = f"{dirs[0]}_{dirs[-1]}"
  path = f"{out_dir}/{filename_pre}.csv"
  weekly_df.to_csv(path)

  with open(f'{out_dir}/{filename_pre}_bad_files.pickle', 'wb') as handle:
    pickle.dump(bad_files, handle, protocol=pickle.HIGHEST_PROTOCOL)

  print(f"DONE with batch ending with {dirs[-1]}")