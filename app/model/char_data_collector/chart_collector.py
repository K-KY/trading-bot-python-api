import requests
import time
import asyncio
import websockets
import json
import ssl


base_url = "https://www.binance.com/fapi/v1/continuousKlines?limit={}&pair={}&contractType=PERPETUAL&interval={}&startTime={}"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"

def collect_chart(limit, coin, interval, time_stamp) :
    url = base_url.format(limit, coin, interval, time_stamp)
    webpage = requests.get(url)


    return webpage.json()

async def listen_binance_kline(coin, interval):
    ssl_context = ssl._create_unverified_context()
    url = BINANCE_WS_URL.format(coin, interval)
    async with websockets.connect(url, ssl=ssl_context) as websocket:
        print("WebSocket 연결됨!")
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            kline = parsed['data']['k']
            print(parsed)

# gettimestamp = int((time.time() - 60 * 60 * 24 * 120) * 1000)  # 120일 전의 타임스탬프
# data = collect_chart(100, "ETHUSDT", "1d", gettimestamp)
# print(data)
#
# if __name__ == "__main__":
#     asyncio.run(listen_binance_kline("btcusdt_perpetual", "1m"))
