import numpy as np
import pandas as pd
import os
import json
import tqdm
from collections import Counter

def extract_from_json(file):
    a = json.load(file)
    df = pd.json_normalize(a['data']['search']['places']['results']['edges'])

