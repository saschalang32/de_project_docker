from kafka import KafkaConsumer
from json import loads
import json
import pymongo as pym

BitcoinConsumer = KafkaConsumer('bitcoin',bootstrap_servers=['broker:29092'],value_deserializer=lambda x: loads(x.decode('utf-8')),)

#Set the connection string
MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/bitcoin?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"

client = pym.MongoClient(MongoSRV)

db = client['projectdb']
bitcoin = db['bitcoin']



for message in BitcoinConsumer:
    for value in message[0].keys():
        value.replace('.','',inplace=True)
        bitcoin.insert_one(message)