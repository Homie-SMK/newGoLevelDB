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
#df = df.fillna(-2)

"""
no_comp_gets = []
no_comp_blockNums = []

single_comp_gets = []
single_comp_blockNums = []

concurrent_comp_gets = []
concurrent_comp_blockNums = []

no_comp_cnt = 0
single_comp_cnt = 0
concurrent_comp_cnt =0

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
        no_comp_blockNums.append(row['BlockNumber'])
        no_comp_cnt = no_comp_cnt + 1
    elif int(row['SourceLevel']) < 10:
        single_comp_gets.append(float(row['GetTime']))
        single_comp_blockNums.append(row['BlockNumber'])
        single_comp_cnt = single_comp_cnt + 1
    elif int(row['SourceLevel']) > 10:
        concurrent_comp_gets.append(float(row['GetTime']))
        concurrent_comp_blockNums.append(row['BlockNumber'])
        concurrent_comp_cnt = concurrent_comp_cnt + 1

print("count of get done in each section of which ranges are 4,500,000th ~ 8,000,000th blocks")
print("No Compaction :: ", no_comp_cnt)
print("Single Compaction :: ", single_comp_cnt)
print("Concurrent Compaction :: ", concurrent_comp_cnt)

df_no_comp = pd.DataFrame({'no_comp_blockNums': no_comp_blockNums,
                   'no_comp_gets': no_comp_gets,})

df_single_comp = pd.DataFrame({'single_comp_blockNums': single_comp_blockNums,
                    'single_comp_gets' : single_comp_gets})

df_concurrent_comp = pd.DataFrame({'concurrent_comp_blockNums': concurrent_comp_blockNums,
                    'concurrent_comp_gets': concurrent_comp_gets})
"""
# Create the scatter plot
fig = px.scatter(df, x='BlockNumber', y='GetTime', color='Compaction', color_discrete_sequence=['blue', 'red', 'green'], opacity=0.7)

# Add labels and title
fig.update_layout(xaxis_title='INDEX', yaxis_title='GET LATENCY (MS)', title='NEW LEVELDB')

fig.update_layout(
#     yaxis = dict(
#         tickmode = 'linear',
#         tick0 = 0,
#         dtick = 50
#     ),
    font=dict(
        family="Times New Roman, monospace",
        size=18,
        color="Black"
    ),
    autosize=False,
    #width=800,
    #height=450,
)

# fig2.update_xaxes(range=[8000000, 9500000])
fig.update_xaxes(range=[7000000, 8000000])
fig.update_yaxes(range=[0, 300])
pio.write_image(fig, './new_get_latency_scatterplot_2.jpeg', scale=6, width=700, height=370)
""",scale=6, width=700, height=370"""