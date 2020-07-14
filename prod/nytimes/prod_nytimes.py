
import requests
from kafka import KafkaProducer
from time import sleep
from json import dumps
import time

def topstories():
  #topic=input("The possible section are  arts, automobiles, books, business, fashion, food, health, home, insider, magazine, movies, national, nyregion, obituaries, opinion, politics, realestate, science, sports, sundayreview, technology, theater, tmagazine, travel, upshot, and world.\n Plesae select one section to choose from :- ")
  topic = 'world'
  requestUrl = f"https://api.nytimes.com/svc/topstories/v2/{topic}.json?api-key=BKXxHribzKstIlWCA3iAhgjAI5cIbLLO"
  requestHeaders = {
    "Accept": "application/json"
  }
  try:
      #   print(requestUrl)
        request = requests.get(requestUrl, headers=requestHeaders)
        if request.status_code!=200:
               raise Exception 
  except Exception:
        print(request.status_code)
        print("Sorry Cannot connect to the server")  
  else:
        global reqjson
   # reqjson=request.json() 
        reqjson=request.json()   
        return reqjson
        
  
  
def kakfapd(reqjson):
#  print(reqjson)     
  producer = KafkaProducer(bootstrap_servers=['broker:29092']
                          ,value_serializer=lambda v: dumps(v).encode('utf8'))
  #  for  key in reqjson :
  #        data={key:value}
  producer.send('nytimes', reqjson)
  #  sleep(5)

  producer.close() 


if __name__ == "__main__":
  time.sleep(130)
  while True:
    try:
      reqjson=topstories()
      kakfapd(reqjson)
      sleep(30)
    except:
      print('Waiting for KafkaBroker')
      time.sleep(60)
      print('Next try!')