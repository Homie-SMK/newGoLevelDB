import plotly.express as px
import pandas as pd
import numpy as np

# Create the data for the plot
x1 = np.random.normal(0, 1, 100)
x2 = np.random.normal(1, 1, 80)
x3 = np.random.normal(2, 1, 120)
y = np.random.normal(0, 1, 100)

# Create a DataFrame with the data and group information
df = pd.DataFrame({'X': np.concatenate([x1, x2, x3]),
                   'Y': np.concatenate([y[:100], y[:80], y[:120]]),
                   'Group': ['X1']*100 + ['X2']*80 + ['X3']*120})

# Create the scatter plot
fig = px.scatter(df, x='X', y='Y', color='Group', custom_data=['Group'])

# Add labels and title
fig.update_layout(xaxis_title='X', yaxis_title='Y', title='Scatter Plot')

# Add hover information
fig.update_traces(hovertemplate='Group=%{customdata[0]}')

# Show the plot
fig.show()