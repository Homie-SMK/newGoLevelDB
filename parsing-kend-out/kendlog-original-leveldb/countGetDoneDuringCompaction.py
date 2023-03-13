from matplotlib import pyplot as plt
import numpy as np
import pandas as pd

## the range is 4,500,000th ~ 8,000,000th block insertion
## print the count of get operations triggered during compaction duration for original leveldb and new leveldb each and draw bar graph

df = pd.read_csv('original.kend.out.trimmed.4500000.to.8000000.parsed.only.statetrie3.csv')

original_get_cnt = (df['Compaction'] == 'Compaction').sum()

print("original leveldb :: the count of get operated during compaction is done :: ", original_get_cnt)

df = pd.read_csv('new.kend.out.trimmed.4500000.to.8000000.parsed.only.statetrie3.csv')

new_get_cnt = (df['Compaction'] == 'Compaction').sum()

print("new leveldb :: the count of get operated during compaction is done :: ", new_get_cnt)

x = ['original', 'new']
y = [original_get_cnt, new_get_cnt]