import pandas as pd
import sqlite3

df = pd.read_feather('data/drom/ftr/renault-arkana.ftr')
#df['index'] = df.id
#df.set_index('index', inplace=True)

#conn = sqlite3.connect('test.sqlite')

#df.to_sql('cars', conn, if_exists='append')
