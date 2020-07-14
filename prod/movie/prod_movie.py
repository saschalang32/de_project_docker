import time
import json 
import requests
from json import dumps
from kafka import KafkaProducer


req=requests.get("https://api.themoviedb.org/3/movie/now_playing?api_key=dfd8fe4364947db1f86bb33a39ecd6bf&language=en-US&page=1")
print(req.status_code) # 200 being printed 
datajson=req.json()

time.sleep(125)

while True:
	try:
		producer = KafkaProducer(bootstrap_servers=['broker:29092'], acks=1,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
		for key,value in datajson.items():
			data={key:value}
			producer.send('movieapi', value=data)
			time.sleep(5)
		producer.flush()
	except:
		time.sleep(60)
		print('Next try!')
