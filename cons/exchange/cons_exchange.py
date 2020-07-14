from kafka import KafkaConsumer
from json import loads
import json
import pymongo as pym
import time



#Set the connection string
MongoSRV = "mongodb+srv://setup_admin:BIgQIsyGh3wt1Hrl@kafkaproject.ip0ti.mongodb.net/bitcoin?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE"

client = pym.MongoClient(MongoSRV)

db = client['projectdb']
bitcoin = db['bitcoin']


time.sleep(127)

while True:
    try:
        BitcoinConsumer = KafkaConsumer('bitcoin',bootstrap_servers=['broker:29092'],value_deserializer=lambda x: loads(x.decode('utf-8')),)
        break
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')
    

for message in BitcoinConsumer:
    bitcoin.insert_one(message.value)