from kafka import KafkaConsumer
from json import loads
import json
import datetime
import pymongo as pym
import pandas as pd
import requests
import time

# moviecons = KafkaConsumer('movieapi',bootstrap_servers=['localhost:9092'],value_deserializer=lambda x: loads(x.decode('utf-8')),)

time.sleep(122)
while True:
    try:
        moviecons=KafkaConsumer('movieapi',
                            bootstrap_servers=['broker:29092'],
                            auto_offset_reset='earliest',
                            enable_auto_commit=True,
                            auto_commit_interval_ms=1000,
                            group_id='movie data',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))
        break
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')
    
MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/nytimes?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
client = pym.MongoClient(MongoSRV)
db=client["projectdb"]
collection=db["nowplayingmovies"]

while True:
    columns = ['PosterPath', 'MovieType','OriginalLanguage','Title','ReleaseDate','OverView']
    dfnowplaying = pd.DataFrame(columns=columns)
    now_playing = requests.get('https://api.themoviedb.org/3/movie/now_playing?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
    now_playing = now_playing.json()
    dataframe_nowplaying = list()
    moviecons = now_playing['results']

    identity  = 0
    for row in moviecons:
        if row['adult'] == False:
            val = "Children"

        if row['adult'] == True:
            val = "Adult"
        
        if row['original_language'] == "en":
            val1 = "English"
        else:
            val1 = row['original_language']
        
        identity = identity + 1

        posts = db.nowplayingmovies
        post_data = {
            #'_id' : identity,
            'poster_path': row['poster_path'],
            'movietype': val,
            'original_language': val1,
            'title': row['title'],
            'release_date': row['release_date'],
            'overview': row['overview']
        }
        posts.insert_one(post_data)

    print('data inserted into nowplayingmovies')