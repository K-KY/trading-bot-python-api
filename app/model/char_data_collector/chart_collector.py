import requests
import time
import asyncio
import websockets
import json
import ssl
import threading
from ta.trend import SMAIndicator
from ta.momentum import StochRSIIndicator
from ta.volatility import BollingerBands
from ta.volume import VolumeWeightedAveragePrice
from pymongo import MongoClient

import app.util.TimeStampConverter as tsc

client = MongoClient("mongodb://root:root@localhost:27017/")
db = client['testDB']
collection = db['test']

BASE_URL = "https://www.binance.com/fapi/v1/continuousKlines"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"

stop_event = {}
threads = {}


# 쓰레드 생성, 작업 할당
def run_monitor_chart(coin: str, interval: str):
    name = "thread-worker_" + str(len(threads) + 1) + "__coin__" + coin + "__interval__" + interval

    event = threading.Event()
    thread = threading.Thread(
        target=__run_listen_kline,
        args=(coin, interval, name),
        name=name,
        daemon=True
    )
    threads[name] = thread
    stop_event[name] = event
    thread.start()


# 쓰레드가 수행할 작업
def __run_listen_kline(coin: str, interval: str, name):
    loop = runner()
    loop.run_until_complete(listen_binance_kline(coin, interval, name))


# 존재하지 않는 코인 검증
async def is_valid_symbol(websocket, name: str):
    try:
        await asyncio.wait_for(websocket.recv(), timeout=5.0)
        return True  # 또는 True 반환도 가능

    except asyncio.TimeoutError:
        print(f"[{name}] 웹소켓 데이터 수신 시간 초과")
        return False  # 또는 False


# binance 웹소켓 연결, 여기에 지표 추가
async def listen_binance_kline(coin: str, interval: str, name: str):
    ssl_context = ssl._create_unverified_context()
    url = BINANCE_WS_URL.format(coin, interval)
    async with websockets.connect(url, ssl=ssl_context) as websocket:
        if not await is_valid_symbol(websocket, name):
            stop_thread(name)

        print(f"WebSocket 연결됨! ({coin}, {interval})")
        while not stop_event[name].is_set():
            data = await websocket.recv()
            parsed = json.loads(data)
            kline = parsed['data']['k']
            kline['t'] = tsc.convert_unix(kline['t'])
            kline['T'] = tsc.convert_unix(kline['T'])
            save_live_data(coin, kline)
            print(kline)
        stop_event[name].is_set()


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


## 쓰레드 개별 종료 로직 개선의 여지가 있음
def stop_thread(thread_name: str):
    if thread_name in stop_event and not threads[thread_name].is_alive():
        return thread_name + " 이미 종료됨"
    if thread_name in stop_event:
        stop_event[thread_name].set()  # 쓰레드 종료 신호
        print(f"[{thread_name}] 종료 요청됨")
        return thread_name + " 종료 요청 완료"
    return thread_name + " 찾을 수 없음"


# 이건 다른 파일로 분리 되어도 될거같음
def runner():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


# gettimestamp = int((time.time() - 60 * 60 * 24 * 120) * 1000)  # 120일 전의 타임스탬프
# data = collect_chart(1000, "ETHUSDT", "1d", gettimestamp)
# for i in range(len(data)) :
#     data[i][0] = tsc.convert_unix(data[i][0])
#     print(data[i])
# # 여기에 지표 분석 로직 추가
#
# if __name__ == "__main__":
#     asyncio.run(listen_binance_kline("btcusdt_perpetual", "1m"))

def save_data(limit: int, coin: str, interval: str, time_stamp: str):
    chart = collect_chart(limit, coin, interval, time_stamp)

    for i in range(len(chart)):
        t = tsc.convert_unix(chart[i][0])
        te = tsc.convert_unix(chart[i][6])
        candle = {
            "t": t,
            "te": te,
            "rsi": 0,
            "macd": 0,
            "sma7": 0,
            "sma20": 0,
            "sma60": 0,
            "sma120": 0,
            "volume": 0,
            "high": chart[i][2],
            "low": chart[i][3],
            "close": chart[i][4]
        }

        # 동일 t 값 존재 여부 확인 후 업데이트 또는 삽입
        result = collection.update_one(
            {
                "symbol": coin,
                f"chart.{interval}.t": t
            },
            {
                "$set": {
                    f"chart.{interval}.$": candle
                }
            }
        )

        if result.matched_count == 0:
            collection.update_one(
                {"symbol": coin},
                {"$push": {f"chart.{interval}": candle}},
                upsert=True
            )


def get_time_stamp_range(interval: str, limit):
    interval = list(interval)
    if interval[1] == 'h':
        return int((time.time() - 60 * 60 * int(interval[0]) * limit) * 1000)
    if interval[1] == 'm':
        return int((time.time() - 60 * int(interval[0]) * limit) * 1000)
    if interval[1] == 'd':
        return int((time.time() - 60 * 60 * 24 * int(interval[0]) * limit) * 1000)
    return int((time.time() - 60 * 60 * 24 * int(interval[0]) * 300) * 1000)


def save_live_data(coin, kline):
    interval = kline['i']  # '1m', '15m', etc.
    t = kline['t']  # 시작 시간
    te = kline['T']  # 끝 시간

    candle = {
        "t": t,
        "te": te,
        "rsi": 0,
        "macd": 0,
        "sma7": 0,
        "sma20": 0,
        "sma60": 0,
        "sma120": 0,
        "volume": 0,
        "high": kline['h'],
        "low": kline['l'],
        "close": kline['c']
    }

    # 1. 동일한 symbol + interval + t 값의 데이터가 있는지 먼저 업데이트 시도
    result = collection.update_one(
        {
            "symbol": coin,
            f"chart.{interval}.t": t
        },
        {
            "$set": {
                f"chart.{interval}.$": candle
            }
        }
    )

    # 2. 없으면 새로 push
    if result.matched_count == 0:
        collection.update_one(
            {"symbol": coin},
            {"$push": {f"chart.{interval}": candle}},
            upsert=True
        )

# 저장할 데이터
# document = {
#     "symbol": "ETHUSDT",
#     "chart": {
#         "1m": [
#             {
#                 "t": 1714368000000,
#                 "te": 1714368059999,
#                 "rsi": 45.2,
#                 "macd": 0.0012,
#                 "sma7": 1873.1,
#                 "sma20": 1872.4,
#                 "sma60": 1870.8,
#                 "sma120": 1865.5,
#                 "volume": 125.43,
#                 "close": 1873.5
#             }
#         ],
#         "15m": [],
#         "1d": []
#     }
# }

# 저장
