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

# the range is 4,500,000th ~ 8,000,000th block insertion
# code that scatter plot which displays get latencies during ranges 
# blue dot for get done during NoCompaction
# red dot for get done during Compaction
# green dot for get done during Concurrent Compaction

# original LevelDB
df = pd.read_csv('kend.out.trimmed.7047333.to.8000000.parsed.only.statetrie3.csv')

no_comp_gets = []
lv0_gets = []
lv1_gets = []
lv2_gets = []
lv3_gets = []
single_comp_gets = []
#concurrent_comp_gets = []
total_gets = []

cnt_no_comp_gets, cnt_lv0_comp_gets, cnt_lv1_comp_gets, cnt_lv2_comp_gets, cnt_lv3_comp_gets, cnt_single_comp_gets, cnt_concurrent_comp_gets, cnt_total_comp_gets = 0, 0, 0, 0, 0, 0, 0, 0

for index, row in df.iterrows():
    #if row['BlockNumber'] == 6103401:
    #    break
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

# Create the figure and axis objects
fig, ax = plt.subplots()

# Create the box plots
bp = ax.boxplot([no_comp_gets, single_comp_gets, total_gets], labels=['NoCompaction', 'Compaction', 'Total'])

# Add labels and title
ax.set_xlabel('SECTION')
ax.set_ylabel('GET LATENCY (MS)')
ax.set_title('ORIGINAL LEVELDB')

# Customize the appearance of the box plots
for box in bp['boxes']:
    box.set(color='blue', linewidth=2)
for whisker in bp['whiskers']:
    whisker.set(color='red', linewidth=2)
for cap in bp['caps']:
    cap.set(color='black', linewidth=2)
for median in bp['medians']:
    median.set(color='green', linewidth=2)
for flier in bp['fliers']:
    flier.set(marker='o', color='yellow', alpha=0.5)

# Show the plot
plt.savefig('original_get_latency_boxplot.png')
