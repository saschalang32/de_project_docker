from kafka import KafkaConsumer
from json import loads
from time import sleep
import pymongo as pym
from pymongo import MongoClient
import datetime

     
consumer=KafkaConsumer('nytimes',
                       bootstrap_servers=['broker:29092'],
                       enable_auto_commit=True,
                       auto_commit_interval_ms=1000,
                       group_id='news articles',
                       value_deserializer=lambda x: loads(x.decode('utf-8')))

MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/nytimes?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"
client = pym.MongoClient(MongoSRV)

database=client["projectdb"]
news=database["top_stories"]

print(news.count_documents({}))

# consumer=empdict.update({consumer})
# print(consumer)
for key in consumer:
#    print(key)
 #   print("\n")
  #  print("\n")
    consumer.commit()
#    news.delete_many({})
    news.insert_one(key.value,{"Date_Entered":str(datetime.date.today())})
KafkaConsumer.close(consumer)    
#print(news.count_documents({}))   