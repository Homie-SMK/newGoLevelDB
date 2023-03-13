import csv
import numpy as np
import pandas as pd
import sys
import re

# code that delete all lines not having "/statetrie/3 table-compaction-start"

logFileName = sys.argv[1]
outFileName = logFileName + ".for.counting.compaction"

with open(logFileName, 'r', encoding='utf-8') as infile:
    with open(outFileName, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if "/statetrie/3 table-compaction-start" in line:
                outfile.write(line) 
