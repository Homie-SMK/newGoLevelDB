# the range is 4,500,000th ~ 8,000,000th block insertion
# scatter plot which displays get latencies during ranges 
# blue dot for get done during NoCompaction
# red dot for get done during Compaction
# green dot for get done during Concurrent Compaction

import csv
import numpy as np
import pandas as pd
import sys
import re
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.io as pio

# the range is 4,500,000th ~ 8,000,000th block insertion
# code that scatter plot which displays get latencies during ranges 
# blue dot for get done during NoCompaction
# red dot for get done during Compaction
# green dot for get done during Concurrent Compaction

# original LevelDB
df = pd.read_csv('kend.out.trimmed.7047333.to.8000000.parsed.only.statetrie3.csv')
"""
no_comp_gets = []
lv0_gets = []
lv1_gets = []
lv2_gets = []
lv3_gets = []
single_comp_gets = []
concurrent_comp_gets = []
total_gets = []

cnt_no_comp_gets, cnt_lv0_comp_gets, cnt_lv1_comp_gets, cnt_lv2_comp_gets, cnt_lv3_comp_gets, cnt_single_comp_gets, cnt_concurrent_comp_gets, cnt_total_comp_gets = 0, 0, 0, 0, 0, 0, 0, 0

for index, row in df.iterrows():
    if row['BlockNumber'] == 7047333:
        continue
    sourceLevel = str(row['SourceLevel']).replace('INFO', ' ')
    sourceLevel = sourceLevel.split(" ")
    row['SourceLevel'] = sourceLevel[0]
    if row['SourceLevel'] == '':
        continue
    if row['Compaction'] == 'NoCompaction':
        no_comp_gets.append(float(row['GetTime']))
        cnt_no_comp_gets = cnt_no_comp_gets + 1
    elif int(row['SourceLevel']) == 0:
        lv0_gets.append(float(row['GetTime']))
        single_comp_gets.append(float(row['GetTime']))
        cnt_lv0_comp_gets = cnt_lv0_comp_gets + 1
        cnt_single_comp_gets = cnt_single_comp_gets + 1
    elif int(row['SourceLevel']) == 1:
        lv1_gets.append(float(row['GetTime']))
        single_comp_gets.append(float(row['GetTime']))
        cnt_lv1_comp_gets = cnt_lv1_comp_gets + 1
        cnt_single_comp_gets = cnt_single_comp_gets + 1
    elif int(row['SourceLevel']) == 2:
        lv2_gets.append(float(row['GetTime']))
        single_comp_gets.append(float(row['GetTime']))
        cnt_lv2_comp_gets = cnt_lv2_comp_gets + 1
        cnt_single_comp_gets = cnt_single_comp_gets + 1
    elif int(row['SourceLevel']) == 3:
        lv3_gets.append(float(row['GetTime']))
        single_comp_gets.append(float(row['GetTime']))
        cnt_lv3_comp_gets = cnt_lv3_comp_gets + 1
        cnt_single_comp_gets = cnt_single_comp_gets + 1
    elif int(row['SourceLevel']) > 10:
        concurrent_comp_gets.append(float(row['GetTime']))
        cnt_concurrent_comp_gets = cnt_concurrent_comp_gets + 1
    total_gets.append(float(row['GetTime']))

print("count of get done in each section of which ranges are 4,500,000th ~ 8,000,000th blocks")
print("No Compaction :: ", cnt_no_comp_gets)
print("Single Compaction :: ", cnt_single_comp_gets)
print("Concurrent Compaction :: ", cnt_concurrent_comp_gets)
"""

cnt_no_compaction, cnt_normal, cnt_concurrent = 0, 0, 0
total_get_latency_no_compaction, total_get_latency_normal, total_get_latency_concurrent = 0, 0, 0

for index, row in df.iterrows():
    if row['Compaction'] == 'NoCompaction':
        cnt_no_compaction = cnt_no_compaction + 1
        total_get_latency_no_compaction = total_get_latency_no_compaction + float(row['GetTime'])
    elif row['Compaction'] == 'Normal':
        cnt_normal = cnt_normal + 1
        total_get_latency_normal = total_get_latency_normal + float(row['GetTime'])
    elif row['Compaction'] == 'Concurrent':
        cnt_concurrent = cnt_concurrent + 1
        total_get_latency_concurrent = total_get_latency_concurrent + float(row['GetTime'])

avg_get_latency_no_compaction = total_get_latency_no_compaction / cnt_no_compaction
avg_get_latency_normal = total_get_latency_normal / cnt_normal
avg_get_latency_concurrent = total_get_latency_concurrent / cnt_concurrent

total_get_latency = total_get_latency_no_compaction + total_get_latency_normal + total_get_latency_concurrent

print("avg get latency of each section")
print("no compaction :: ", avg_get_latency_no_compaction)
print("normal :: ", avg_get_latency_normal)
print("concurrent :: ", avg_get_latency_concurrent)

print("count of get per each section")
print("no compaction :: ", cnt_no_compaction)
print("normal :: ", cnt_normal)
print("concurrent :: ", cnt_concurrent)

cnt_get_total = cnt_no_compaction + cnt_normal + cnt_concurrent

print("total count of compaction :: ", cnt_get_total )
print("total get latency :: ", total_get_latency)


"""
fig = px.box(df, x=df['Compaction'], y=df['GetTime'], log_y=True)
fig.update_layout(
    plot_bgcolor='white',
    font=dict(
        family="Times New Roman, monospace",
        size=18,
        color="Black"
    ),
    autosize=False,
    width=700,
    height=370,
)
fig.update_yaxes(
    yaxis_range=[0,300]
    title="<b>GET Latency (ms)</b>",
    gridcolor='lightgrey'
)
fig.update_xaxes(
    title="<b>Section</b>",
#     gridcolor='lightgrey'
)
#fig.write_image('new_get_latency_boxplot_2.png')
pio.write_image(fig, './new_get_latency_boxplot_2.jpeg',scale=6, width=700, height=370)
"""