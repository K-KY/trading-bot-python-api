import requests
import time
import asyncio
import websockets
import json
import ssl
import threading

BASE_URL = "https://www.binance.com/fapi/v1/continuousKlines"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"

stop_event = threading.Event()
threads = {}

#쓰레드 생성, 작업 할당
def run_monitor_chart(coin: str, interval: str):
    name = "thread-worker " + str(len(threads) + 1) + ",coin = " + coin + ",interval = " + interval
    thread = threading.Thread(
        target=__run_listen_kline,
        args=(coin, interval),
        name=name,
        daemon=True
    )
    threads[name] = thread
    thread.start()

#쓰레드가 수행할 작업
def __run_listen_kline(coin: str, interval: str):
    loop = runner()
    loop.run_until_complete(listen_binance_kline(coin, interval))

# binance 웹소켓 연결, 여기에 지표 추가
async def listen_binance_kline(coin: str, interval: str):
    ssl_context = ssl._create_unverified_context()
    url = BINANCE_WS_URL.format(coin, interval)
    async with websockets.connect(url, ssl=ssl_context) as websocket:
        print(f"WebSocket 연결됨! ({coin}, {interval})")
        while True:
            data = await websocket.recv()
            parsed = json.loads(data)
            kline = parsed['data']['k']
            print(kline)


# 과거 차트 데이터 조회
def collect_chart(limit: int, coin: str, interval: str, time_stamp: str):
    params = {
        "limit": limit,
        "pair": coin,
        "contractType": "PERPETUAL",
        "interval": interval,
        "startTime": time_stamp
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()


## 작업중인 쓰레드 반환
def get_all_workers():
    return [
        {
            "name": name,
            "alive": t.is_alive(),
            "ident": t.ident
        }
        for name, t in threads.items()
    ]

def runner():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop

# gettimestamp = int((time.time() - 60 * 60 * 24 * 120) * 1000)  # 120일 전의 타임스탬프
# data = collect_chart(1000, "ETHUSDT", "1d", gettimestamp)
# print(data)
# 여기에 지표 분석 로직 추가

# data = collect_chart(100, "ETHUSDT", "1d", gettimestamp)
# print(data)
#
# if __name__ == "__main__":
#     asyncio.run(listen_binance_kline("btcusdt_perpetual", "1m"))
