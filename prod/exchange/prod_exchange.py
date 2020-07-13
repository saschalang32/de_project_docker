import time
from datetime import datetime
import json
from bson import json_util
from kafka import KafkaProducer
import requests



while True:
    try:
        producer = KafkaProducer(bootstrap_servers='broker:29092')
        url = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=EUR&apikey=P7BXV6ZF20J02028'
        r = requests.get(url)
        producer.send('bitcoin', json.dumps(r.json(), default=json_util.default).encode('utf-8'))
        time.sleep(30)
    except:
        print('Waiting for KafkaBroker')
        time.sleep(30)
        print('Next try!')
    