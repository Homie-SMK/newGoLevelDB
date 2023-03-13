import csv
import numpy as np
import pandas as pd
import sys

#print(sys.argv)

txtFileName = sys.argv[1]
outCSVName = txtFileName + ".csv"

colnames=['GetTime', 'LevelDBPath', 'Compaction', 'SourceLevel', 'BlockNumber']

df = pd.read_csv(txtFileName, sep=r"\s+", names=colnames, index_col=None)
df.to_csv(outCSVName, index=None)
#print(df.head())

