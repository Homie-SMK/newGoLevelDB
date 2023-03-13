import csv
import numpy as np
import pandas as pd
import sys
import re
import matplotlib.pyplot as plt

df = pd.read_csv('kend.out.trimmed.7047333.to.8000000.parsed.only.statetrie3.csv')

cnt_no_compaction, cnt_normal = 0, 0
total_get_latency_no_compaction, total_get_latency_normal = 0, 0

for index, row in df.iterrows():
    if row['Compaction'] == 'NoCompaction':
        cnt_no_compaction = cnt_no_compaction + 1
        total_get_latency_no_compaction = total_get_latency_no_compaction + float(row['GetTime'])
    elif row['Compaction'] == 'Compaction':
        cnt_normal = cnt_normal + 1
        total_get_latency_normal = total_get_latency_normal + float(row['GetTime'])

avg_get_latency_no_compaction = total_get_latency_no_compaction / cnt_no_compaction
avg_get_latency_normal = total_get_latency_normal / cnt_normal
