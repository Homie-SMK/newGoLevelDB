import csv
import numpy as np
import pandas as pd
import sys
import re
import matplotlib.pyplot as plt

# new leveldb

newLogFileName = sys.argv[1]

cnt_single, cnt_concurrent, cnt_lv0, cnt_lv1, cnt_lv2, cnt_lv3, cnt_total = 0, 0, 0, 0, 0, 0, 0

with open(newLogFileName, 'r', encoding='utf-8') as infile:
    for line in infile:
        splitLine = line.split(" ")
        sourceLevel = splitLine[5].replace('SM', '')
        sourceLevel = sourceLevel.replace('INFO', ' ')
        sourceLevel = sourceLevel.split(" ")
        sourceLevel = sourceLevel[0]
        #sourceLevel = sourceLevel.replace('\n', '')
        if sourceLevel == '':
            continue
        sourceLevel = int(sourceLevel)
        
        if sourceLevel < 10:
            cnt_single = cnt_single + 1
            if sourceLevel == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif sourceLevel == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif sourceLevel == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif sourceLevel == 3:
                cnt_lv3 = cnt_lv3 + 1
        elif 10 < sourceLevel and sourceLevel < 100:
            cnt_concurrent = cnt_concurrent + 1
            lv_tmp1 = sourceLevel/10 
            lv_tmp2 = sourceLevel%10
            
            if lv_tmp1 == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif lv_tmp1 == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif lv_tmp1 == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif lv_tmp1 == 3:
                cnt_lv3 = cnt_lv3 + 1
            
            if lv_tmp2 == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif lv_tmp2 == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif lv_tmp2 == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif lv_tmp2 == 3:
                cnt_lv3 = cnt_lv3 + 1              
        elif sourceLevel > 100:
            cnt_concurrent = cnt_concurrent + 1
            lv_tmp1 = sourceLevel/100
            lv_tmp2 = (sourceLevel%100)/10
            lv_tmp3 = sourceLevel%10
            
            if lv_tmp1 == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif lv_tmp1 == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif lv_tmp1 == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif lv_tmp1 == 3:
                cnt_lv3 = cnt_lv3 + 1
                
            if lv_tmp2 == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif lv_tmp2 == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif lv_tmp2 == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif lv_tmp2 == 3:
                cnt_lv3 = cnt_lv3 + 1
            
            if lv_tmp3 == 0:
                cnt_lv0 = cnt_lv0 + 1
            elif lv_tmp3 == 1:
                cnt_lv1 = cnt_lv1 + 1
            elif lv_tmp3 == 2:
                cnt_lv2 = cnt_lv2 + 1
            elif lv_tmp3 == 3:
                cnt_lv3 = cnt_lv3 + 1
                
print("compaction done in new leveldb during 4,500,000th ~ 8,000,000th blocks")
print("level0 :: ", cnt_lv0)
print("level1 :: ", cnt_lv1)
print("level2 :: ", cnt_lv2)
print("level3 :: ", cnt_lv3)
print("total_single :: ", cnt_single)
print("total_concurrent :: ", cnt_concurrent)

cnt_total = cnt_single + cnt_concurrent
print("total :: ", cnt_total)

x = ['level0', 'level1', 'level2', 'level3', 'single', 'concurrent', 'total']
y = [cnt_lv0, cnt_lv1, cnt_lv2, cnt_lv3, cnt_single, cnt_concurrent, cnt_total]

plt.bar(x, y)

plt.xlabel('LEVELS')
plt.ylabel('COUNT OF COMPACTION')
plt.title('NEW LEVELDB')

plt.savefig('new_count_of_compaction.png')