import requests

url = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=EUR&apikey=DEMO'
r = requests.get(url)



hihi =  r.json()
print(type(hihi))
print(hihi['Realtime Currency Exchange Rate']['1. From_Currency Code'])
#print(hihi.items())



