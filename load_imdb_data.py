# -*- coding: utf-8 -*-

# Project hopes:
# Scrape imdb for 20,000 movie_titles, ratings, and whatever other features
# save into a .csv file
# load that .csv into a SQL database (IBM or MySql?)
# browse the data and make analyses
# visulatize my findings
# upload the project to Github 
#/usr/bin/env python


## Project:
## answer the question "Does having more than 1 director increase the rating of a movie?"

# load data in from 2 IMDB datasets (.csv format)
# clean the data using pandas (drop nulls?)
# load into Sql?  (google platform or sql lite) 
# query using python + api
# analyze the data and visualize with plotly

import csv 
from itertools import islice
import pandas as pd
from tqdm import tqdm
import mysql.connector as sql
from mysql.connector.constants import ClientFlag

config = {
    'user': '----',
    'password': '----',
    'host': '----',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': '../keys/server-ca.pem',
    'ssl_cert': '../keys/client-cert.pem',
    'ssl_key': '../keys/client-key.pem'
}

# now we establish our connection
config['database'] = 'imdb_proj'  # add new database to config dict
cnxn = sql.connect(**config)
cursor = cnxn.cursor()

cursor.execute(
    """CREATE TABLE IF NOT EXISTS reviews (
    title_id VARCHAR(255),
    avg_rating DOUBLE,
    num_votes INT )""")

cursor.execute(
    """CREATE TABLE IF NOT EXISTS movie_titles (
    title_id VARCHAR(255),
    ordering INT,
    title LONGTEXT,
    region VARCHAR(100),
    language VARCHAR(100),
    types VARCHAR(255),
    attributes VARCHAR(255),
    is_original_title INT )""")

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("TSV empty. Finishing execution")
        return False 

    # Check for nulls in first column (the ID)
    if df[0].isnull().values.any():
        raise Exception("Null values found")

    return True

def check_if_df_already_loaded(df: pd.DataFrame, sql_table_name: str) -> bool:
    # Compare length of df to length of SQL table
    len_of_pd_table = len(df.index)-1
    cursor.execute(f"SELECT COUNT(*) from {sql_table_name}")
    len_of_sql_table = cursor.fetchall()[0][0]
    print(len_of_sql_table)

    if len_of_pd_table == len_of_sql_table and len_of_sql_table > 0:
        print('DB already loaded')
        return True

    return False

# Validate (Transform) and Load
if check_if_valid_data(ratings_df):
    print("Data valid, proceed to Load stage")

if check_if_df_already_loaded(ratings_df, 'reviews'):
    print("Data already loaded")
else:
    print("Loading data")

    query = ("INSERT INTO reviews (title_id, avg_rating, num_votes) "
             "VALUES (%s, %s, %s)")
    cursor.executemany(query, list(ratings_df.to_records(index=False))[1:])
    cnxn.commit()  # and commit changes

# if check_if_valid_data(titles_df):
#     print("Data valid, proceed to Load stage")

# if check_if_df_already_loaded(titles_df, 'movie_titles'):
#     print("Data already loaded")
# else:
#     print("Loading data")

#     query = (
#     """INSERT INTO movie_titles (title_id, ordering, title, region, 
#     language, types, attributes, is_original_title
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")
#     cursor.executemany(query, list(titles_df.to_records(index=False))[1:])
#     cnxn.commit()  # and commit changes



cursor.execute("SELECT * FROM movie_titles LIMIT 5")

out = cursor.fetchall()
for row in out:
    print(row)

cnxn.close()