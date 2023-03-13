import csv
import numpy as np
import pandas as pd
import sys
import re


logFileName = sys.argv[1]
outFileName = logFileName + ".only.statetrie3"

with open(logFileName, 'r', encoding='utf-8') as infile:
    with open(outFileName, 'w', encoding='utf-8') as outfile:
        for line in infile:
            if "statetrie/3" in line:
                outfile.write(line) 
