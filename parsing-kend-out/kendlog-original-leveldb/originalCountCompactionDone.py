import csv
import numpy as np
import pandas as pd
import sys
import re
import matplotlib.pyplot as plt

# the range is 4,500,000th ~ 8,000,000th block insertion
# print the count of compaction done 
# original leveldb : the total count of compaction done
# new leveldb : the count of single compaction :: concurrent compaction with 2 levels :: concurrent compaction with 3 levels
# draw bar graph

originalLogFileName = sys.argv[1]

cnt_lv0, cnt_lv1, cnt_lv2, cnt_lv3 = 0, 0, 0, 0

# original leveldb
with open(originalLogFileName, 'r', encoding='utf-8') as infile:
    for line in infile:
        splitLine = line.split(" ")
        sourceLevel = splitLine[5].replace('\n', '')
        
        if sourceLevel == '0':
            cnt_lv0 = cnt_lv0 + 1
        elif sourceLevel == '1':
            cnt_lv1 = cnt_lv1 + 1
        elif sourceLevel == '2':
            cnt_lv2 = cnt_lv2 + 1
        elif sourceLevel == '3':
            cnt_lv3 = cnt_lv3 + 1


cnt_total = cnt_lv0 + cnt_lv1 + cnt_lv2 + cnt_lv3

print("compaction done in original leveldb during 4,500,000th ~ 8,000,000th blocks")
print("level0 :: ", cnt_lv0)
print("level1 :: ", cnt_lv1)
print("level2 :: ", cnt_lv2)
print("level3 :: ", cnt_lv3)
print("total :: ", cnt_total)
print()

x = ['level0', 'level1', 'level2', 'level3', 'total']
y = [cnt_lv0, cnt_lv1, cnt_lv2, cnt_lv3, cnt_total]

plt.bar(x, y)

plt.xlabel('LEVELS')
plt.ylabel('COUNT')
plt.title('ORIGINAL LEVELDB')

plt.savefig('original_count_of_compaction.png')