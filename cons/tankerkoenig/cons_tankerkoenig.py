from kafka import KafkaConsumer
from json import loads
import json
import pymongo as pym
import time


time.sleep(138)

while True:
    try:
        FuelConsumer = KafkaConsumer('tankerkoenig',bootstrap_servers=['broker:29092'],value_deserializer=lambda x: loads(x.decode('utf-8')),)
        break
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')




#Set the connection string
MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/tankerkoenig?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"

client = pym.MongoClient(MongoSRV)

db = client['projectdb']
collection_fuel = db['collection_fuel']






for message in FuelConsumer:
    print('Inserting new data')
    collection_fuel.insert_one(message.value)