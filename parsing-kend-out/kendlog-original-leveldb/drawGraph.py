import sqlite3
import csv
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import iplot
import plotly.io as pio
import seaborn as sns

df = pd.read_csv('kend.out.trimmed.4500000.to.8000000.parsed.only.statetrie3.csv')

fig = go.Figure()
fig = px.box(df, x=df['Compaction'], y=df['GetTime'], log_y=True)
fig.update_layout(
    plot_bgcolor='white',
    font=dict(
        family="Times New Roman, monospace",
        size=18,
        color="Black"
    ),
    autosize=False,
    width=700,
    height=370,
)
fig.update_yaxes(
    title="<b>GET Latency (ms)</b>",
    gridcolor='lightgrey'
)
fig.update_xaxes(
    title="<b></b>",
#     gridcolor='lightgrey'
)
#fig.show()

pio.write_image(fig, './fig.jpeg',scale=6, width=700, height=370)