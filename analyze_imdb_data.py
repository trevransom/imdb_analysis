# -*- coding: utf-8 -*-
## Project:
## answer the question "Does having a movie available in more languages have an effect on the rating of a movie?"

# query using python
# analyze the data and visualize with plotly

## answer the question "Does having more than 1 translation increase the rating of a movie?"
# in order to answer this question 

# need to join the databases together by id in order to access the ratings for a movie

# maybe I could make a view first where I count the number of translations per movie id
# then i could take that simplified info and associate it with the ratings table giving us our combined desired view

from itertools import islice
import pandas as pd
from tqdm import tqdm
import mysql.connector
from mysql.connector.constants import ClientFlag
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

config = {
    'user': 'root',
    'password': 'xN#DfQ3a693o',
    'host': '35.228.161.110',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': '../keys/server-ca.pem',
    'ssl_cert': '../keys/client-cert.pem',
    'ssl_key': '../keys/client-key.pem'
}

# now we establish our connection
config['database'] = 'imdb_proj'  # add new database to config dict
cnxn = mysql.connector.connect(**config)
cursor = cnxn.cursor()

pd.set_option("display.precision", 3)

df = pd.read_sql('SELECT * FROM movie_titles', con=cnxn)
print(df.head())

df = df.groupby('title_id', as_index=False)['ordering'].max()
df.rename({'ordering': 'num_of_translations'}, axis=1, inplace=True)
print(df.head())

df2 = pd.read_sql('SELECT * FROM reviews', con=cnxn)

df_inner = pd.merge(df, df2, on='title_id', how='inner')
df_inner = df_inner.groupby('num_of_translations', as_index=False)['avg_rating'].mean()
print(df_inner.head())

cnxn.close()

fig = px.bar(df_inner, x='num_of_translations', y='avg_rating')
fig.show()
