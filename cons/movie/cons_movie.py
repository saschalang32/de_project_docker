import datetime
import json
import sys
from json import loads
import pymongo
import requests
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'movieapi',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group')
# value_deserializer=lambda x:loads(x.decode('utf-8'))


try:
    print("Movie and its Revenue")
    for message in consumer:
        consumer.commit()
        print(message.value)
        # print(message.key)
        # print(message.partition)
        # print(message.topic)
        # print("topic has been recived by movieapi")
        # print(consumer)
        # print(message)
    KafkaConsumer.close(consumer)

except Exception as E:
        print('Error : ', E)

