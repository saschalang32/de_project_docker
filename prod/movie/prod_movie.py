import json
# from json import loads
from pymongo import MongoClient
import pymongo
import sys, os
import pandas as pd
import requests
import time
from time import sleep
from kafka import KafkaProducer
import pymongo as pym


MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/movie?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
client = pym.MongoClient(MongoSRV)
db = client['projectdb']
collection = db["nowplayingmovies"]
collection1 = db["popularmovies"]
collection2 = db["toprated"]
collection3 = db["upcomingmovies"]
print("successfully connected to mongodb")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], acks=1,value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# # ###############################Now Playing#########################################
while True:
    try:
        columns = ['PosterPath', 'MovieType','OriginalLanguage','Title','ReleaseDate','OverView']
        dfnowplaying = pd.DataFrame(columns=columns)
        now_playing = requests.get('https://api.themoviedb.org/3/movie/now_playing?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
        now_playing = now_playing.json()
        dataframe_nowplaying = list()
        now_playing_films = now_playing['results']


        identity  = 0

        for row in now_playing_films:
            dfnowplaying.loc[len(dfnowplaying)]= [row['poster_path'],row['adult'],row['original_language'],row['title'],row['release_date'],row['overview']]
            
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
                '_id' : identity,
                'poster_path': row['poster_path'],
                'movietype': val,
                'original_language': val1,
                'title': row['title'],
                'release_date': row['release_date'],
                'overview': row['overview']
            }
            result = posts.insert_one(post_data)
            poster_path = row['poster_path']
            movietype = val
            original_language = val1
            title = row['title']
            release_date = row['release_date']
            overview = row['overview']
            producer.send('movieapi', value=poster_path + "  " + str(movietype) + "  " + original_language + "  " + title + "  " + release_date + "  " + overview)
            time.sleep(2)
    
        print('data inserted into nowplayingmovies')
    except:
        print(Exception())

    # ## ###############################Popular#########################################

    try:
        columns = ['PosterPath', 'MovieType','OriginalLanguage','Title','ReleaseDate','OverView']
        dfpopular = pd.DataFrame(columns=columns)
        popular = requests.get('https://api.themoviedb.org/3/movie/popular?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
        popular = popular.json()
        dataframe_popular = list()
        popular_films = popular['results']


        identity  = 0

        for data in popular_films:
            dfpopular.loc[len(dfpopular)]= [data['poster_path'],data['adult'],data['original_language'],data['title'],data['release_date'],data['overview']]
            
            if data['adult'] == False:
                    val = "Children"

            if data['adult'] == True:
                    val = "Adult"
            
            if data['original_language'] == "en":
                    val1 = "English"
            else:
                    val1 = data['original_language']
            
            identity = identity + 1

            posts1 = db.popularmovies
            post_data1 = {
                '_id' : identity,
                'poster_path': data['poster_path'],
                'movietype': val,
                'original_language': val1,
                'title': data['title'],
                'release_date': data['release_date'],
                'overview': data['overview']
            }       

            result1 = posts1.insert_one(post_data1)

            poster_path = data['poster_path']
            movietype = val
            original_language = val1
            title = data['title']
            release_date = data['release_date']
            overview = data['overview']
            producer.send('movieapi', value=poster_path + "  " + str(movietype) + "  " + original_language + "  " + title + "  " + release_date + "  " + overview)
            time.sleep(2)
            # datapopular = (poster_path,adult,original_language,title,release_date,overview)
            # dataframe_popular.append(datapopular)


        print("data inserted into popularmovies")
    except:
        print(Exception())

    # #################################Top Rated#########################################

    try:

        columns = ['PosterPath', 'MovieType','OriginalLanguage','Title','ReleaseDate','OverView']
        dftoprated = pd.DataFrame(columns=columns)
        toprated = requests.get('https://api.themoviedb.org/3/movie/top_rated?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
        toprated = toprated.json()
        dataframe_toprated= list()
        toprated_films = toprated['results']


        identity  = 0

        for datatoprated in toprated_films:
            dftoprated.loc[len(dftoprated)]= [datatoprated['poster_path'],datatoprated['adult'],datatoprated['original_language'],datatoprated['title'],datatoprated['release_date'],datatoprated['overview']]
            
            if datatoprated['adult'] == False:
                    val = "Children"

            if datatoprated['adult'] == True:
                    val = "Adult"
            
            if datatoprated['original_language'] == "en":
                    val1 = "English"
            else:
                    val1 = datatoprated['original_language']
            
            identity = identity + 1

            posts2 = db.toprated
            post_data2 = {
                '_id' : identity,
                'poster_path': datatoprated['poster_path'],
                'movietype': val,
                'original_language': val1,
                'title': datatoprated['title'],
                'release_date': datatoprated['release_date'],
                'overview': datatoprated['overview']
            }       

            result2 = posts2.insert_one(post_data2)

            
            poster_path = datatoprated['poster_path']
            movietype = val
            original_language = val1
            title = datatoprated['title']
            release_date = datatoprated['release_date']
            overview = datatoprated['overview']
            producer.send('movieapi', value=poster_path + "  " + str(movietype) + "  " + original_language + "  " + title + "  " + release_date + "  " + overview)
            time.sleep(2)
            # datatoprated = (poster_path,adult,original_language,title,release_date,overview)
            # dataframe_toprated.append(datatoprated)

        print('data inserted into toprated movies')
    except:
        print(Exception())
    
    # #################################Upcoming#########################################

    try:
        columns = ['PosterPath', 'MovieType','OriginalLanguage','Title','ReleaseDate','OverView']
        dfupcoming = pd.DataFrame(columns=columns)
        upcoming = requests.get('https://api.themoviedb.org/3/movie/upcoming?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1')
        upcoming = upcoming.json()
        dataframe_upcoming= list()
        upcoming_films = upcoming['results']


        identity  = 0

        for dataupcoming in upcoming_films:
            dfupcoming.loc[len(dfupcoming)]= [dataupcoming['poster_path'],dataupcoming['adult'],dataupcoming['original_language'],dataupcoming['title'],dataupcoming['release_date'],dataupcoming['overview']]
            
            if dataupcoming['adult'] == False:
                    val = "Children"

            if dataupcoming['adult'] == True:
                    val = "Adult"
            
            if dataupcoming['original_language'] == "en":
                    val1 = "English"
            else:
                    val1 = dataupcoming['original_language']
            
            identity = identity + 1

            posts3 = db.upcomingmovies
            post_data3 = {
                '_id' : identity,
                'poster_path': dataupcoming['poster_path'],
                'movietype': val,
                'original_language': val1,
                'title': dataupcoming['title'],
                'release_date': dataupcoming['release_date'],
                'overview': dataupcoming['overview']
            }       

            result3 = posts3.insert_one(post_data3)

            
            poster_path = dataupcoming['poster_path']
            movietype = val
            original_language = val1
            title = dataupcoming['title']
            release_date = dataupcoming['release_date']
            overview = dataupcoming['overview']
            producer.send('movieapi', value=poster_path + "  " + str(movietype) + "  " + original_language + "  " + title + "  " + release_date + "  " + overview)
            time.sleep(2)
            # dataupcoming = (poster_path,adult,original_language,title,release_date,overview)
            # dataframe_upcoming.append(dataupcoming)

        
        print('data inserted into upcomingmovies')

    except:
        print(Exception())
    time.sleep(60)
