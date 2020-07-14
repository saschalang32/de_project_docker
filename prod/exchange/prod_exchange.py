import time
from datetime import datetime
import json
from bson import json_util
from kafka import KafkaProducer
import requests

def rep(giv_dict):
    for i in giv_dict.values():
        for j in list(i):
            i[j.replace("."," ")]=i[j]
            i.pop(j)
        for item in giv_dict:
            giv_dict[item]=i
        return giv_dict

time.sleep(120)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='broker:29092')
        url = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=EUR&apikey=P7BXV6ZF20J02028'
        r = requests.get(url)
        data = rep(r.json())
        producer.send('bitcoin', json.dumps(data, default=json_util.default).encode('utf-8'))
        time.sleep(30)
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')
    