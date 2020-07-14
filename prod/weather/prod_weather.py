from kafka import KafkaProducer
import json
import time
import requests


Openweathermap_ApiUrl="http://api.openweathermap.org/data/2.5/weather?lat=49.413892&lon=8.6488633&appid=1d75cbaae17ddcf0e9774cf9a33b4581"


def GetPayload(ApiUrl):
    if requests.get(ApiUrl).status_code == 200:
        return requests.get(ApiUrl).content
    else:
        print("API Service is down")


def Producer(TopicName,datastream):
    for row in datastream:
        #print(json.dumps(row).encode('utf-8'))
        producer.send(TopicName, json.dumps(row).encode('utf-8'))
        time.sleep(1)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='broker:29092')
        #Get response from Openweather API
        OpenweathermapRequest = GetPayload(Openweathermap_ApiUrl)
        #Prepare stream for weather data
        load=[]
        load.append(json.loads(OpenweathermapRequest))
        #Send to Kafka broker
        Producer('weatherdata',load)
        time.sleep(360)
    except:
        print('Waiting for KafkaBroker')
        time.sleep(60)
        print('Next try!')