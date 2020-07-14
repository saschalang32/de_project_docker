import time
from datetime import datetime
import json
from bson import json_util
from kafka import KafkaProducer
import requests



time.sleep(135)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='broker:29092')
        url = 'https://creativecommons.tankerkoenig.de/json/list.php?lat=49.413847&lng=8.651242&rad=10&sort=price&type=diesel&apikey=1541b4b5-e612-4a2b-73f7-467fa9f70bde'
        r = requests.get(url)
        producer.send('tankerkoenig', json.dumps(r.json(), default=json_util.default).encode('utf-8'))
        print(f'New update sent {datetime.now()}')
        time.sleep(360)
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')
