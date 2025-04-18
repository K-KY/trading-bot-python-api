import requests
import time

base_url = "https://www.binance.com/fapi/v1/continuousKlines?limit={}&pair={}&contractType=PERPETUAL&interval={}&startTime={}"

def collect_chart(limit, coin, interval, time_stamp) :
    url = base_url.format(limit, coin, interval, time_stamp)
    webpage = requests.get(url)


    return webpage.json()

gettimestamp = int((time.time() - 60 * 60 * 24 * 120) * 1000)  # 120일 전의 타임스탬프
data = collect_chart(100, "ETHUSDT", "1d", gettimestamp)
print(data)