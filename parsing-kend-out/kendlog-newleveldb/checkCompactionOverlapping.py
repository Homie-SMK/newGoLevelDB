import csv
import numpy as np
import pandas as pd
import sys

#print(sys.argv)

logFileName = sys.argv[1]
#outFileName = logFileName + ".trim"
cnt = 0
loc = 0
with open(logFileName, 'r', encoding='utf-8') as infile:
    compactionStarted = False
    isOverlapping = False
    for line in infile:
        if "table-compaction-start" in line:
            if compactionStarted :
                isOverlapping = True
                loc = cnt
            if "/chaindata/statetrie/3 table-compaction-start" in line:
                compactionStarted = True            
        if "/chaindata/statetrie/3 table-compaction-ended" in line:
            compactionStarted = False
        cnt = cnt + 1
print(isOverlapping)
print(loc)

        
            