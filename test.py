import requests

url = 'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=EUR&apikey=DEMO'
r = requests.get(url)

def rep(giv_dict):
    for i in giv_dict.values():
        for j in list(i):
            i[j.replace("."," ")]=i[j]
            i.pop(j)
        for item in giv_dict:
            giv_dict[item]=i
        return giv_dict

hihi =  r.json()

haha = rep(hihi)
print(haha)
#print(type(hihi))
print(haha['Realtime Currency Exchange Rate'])
#print(hihi.items())



