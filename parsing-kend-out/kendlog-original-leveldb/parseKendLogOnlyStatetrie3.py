import csv
import numpy as np
import pandas as pd
import sys
import re


logFileName = sys.argv[1]
outFileName = logFileName + ".parsed.only.statetrie3"

with open(logFileName, 'r', encoding='utf-8') as infile:
    compactionFlag = 0
    sourceLevel = "-1"
    blockNum = ""
    dbPath = ""
    getLatency = float(0)
    with open(outFileName, 'w', encoding='utf-8') as outfile:
        for line in infile:
            isCase = False
            if "/statetrie/3 table-compaction-start" in line: # example line: "Compaction start sourceLevel 1 statetrie/2"
                isCase = True
                compactionFlag = 1
                splitLine = line.split(" ")
                if "get-elapsed" in line:
                    sourceLevel = splitLine[5].replace('SM', '')
                else:
                    sourceLevel = splitLine[5].replace('\n', '')

            if "/statetrie/3 table-compaction-ended" in line: # example line: "Compaction end sourceLevel 1 statetrie/2"
                isCase = True
                compactionFlag = 0
                sourceLevel = "-1" # back to default

            if "/statetrie/3 get-elapsed" in line: # example line: "SMIN Gettime 11.123 statetrie/2"
                isCase = True
                splitLine = line.split(" ")
                # print(splitLine)
                if "table-compaction-start" in line:
                    getlat = splitLine[9].replace('\n', '')
                else:    
                    getlat = splitLine[4].replace('\n', '') # put right index based on the format of kend.out log
                getLatency = float(getlat) / 1000 # microseconds to milliseconds
                getlat = "%.5f" % getLatency
                dbPath = splitLine[2].replace('y/chaindata/', '')

            if "Inserted a new block" in line:
                delimiters = " ="
                splitLine = re.split("|".join(delimiters), line)
                #print(splitLine)
                blockNum = splitLine[29]
            
            outLine = ""
            if isCase:
                if compactionFlag == 1:
                    outLine = getlat + " " + dbPath + " " + "Compaction" + " " + sourceLevel + " " + blockNum
                else:
                    outLine = getlat + " " + dbPath + " " + "NoCompaction" + " " + "-1" + " " + blockNum # sourceLevel -1 if no compaction is going on
            else:
                continue
            outfile.write(outLine+"\n") 
