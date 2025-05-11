import pandas as pd
import requests
import time
import ta as ta
from ta.trend import SMAIndicator
from ta.momentum import StochRSIIndicator
from ta.volatility import BollingerBands
from ta.volume import VolumeWeightedAveragePrice
from pymongo import MongoClient

import app.util.TimeStampConverter as tsc

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

client = MongoClient("mongodb://root:root@localhost:27017/")
db = client['testDB']
collection = db['test']

BASE_URL = "https://www.binance.com/fapi/v1/continuousKlines"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"


# 과거 차트 데이터 조회
# 데이터 프레임을 리턴
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
    return calculate_ta(response.json())


def convert_data(chart):
    candles = []
    for i in range(len(chart)):
        t = tsc.convert_unix(chart[i][0])
        te = tsc.convert_unix(chart[i][6])
        candle = {
            "t": t,
            "te": te,
            "rsi": 0,
            "macd": 0,
            "signal": 0,
            "sma7": 0,
            "sma20": 0,
            "sma60": 0,
            "sma120": 0,
            "volume": chart[i][5],
            "high": chart[i][2],
            "low": chart[i][3],
            "close": chart[i][4]
        }
        candles.append(candle)
    return candles


def save_data(chart, coin, interval):
    for i in range(len(chart)):

        # 동일 t 값 존재 여부 확인 후 업데이트 또는 삽입
        result = collection.update_one(
            {
                "symbol": coin,
                f"chart.{interval}.t": chart[i][0]
            },
            {
                "$set": {
                    f"chart.{interval}.$": chart[i]
                }
            }
        )

        if result.matched_count == 0:
            collection.update_one(
                {"symbol": coin},
                {"$push": {f"chart.{interval}": chart[i]}},
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
        "signal": 0,
        "sma7": 0,
        "sma20": 0,
        "sma60": 0,
        "sma120": 0,
        "volume": kline['v'],
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


def calculate_ta(data):
    candles = convert_data(data)

    frame = pd.DataFrame(candles)
    frame.rename(
        columns={0: 'ts', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume', 6: 'te', 7: 'rsi', 8: 'signal'},
        inplace=True)
    frame['close'] = frame['close'].astype(float)
    frame['sma7'] = ta.trend.sma_indicator(frame['close'], window=7)
    frame['sma20'] = ta.trend.sma_indicator(frame['close'], window=20)
    frame['sma60'] = ta.trend.sma_indicator(frame['close'], window=60)
    frame['sma120'] = ta.trend.sma_indicator(frame['close'], window=120)
    frame['rsi'] = ta.momentum.RSIIndicator(close=frame['close'], window=14).rsi()
    frame['macd'] = ta.trend.macd(frame['close'])
    frame['signal'] = ta.trend.macd_signal(frame['close'])
    return frame

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
# gettimestamp = int((time.time() - 60 * 60 * 24 * 300) * 1000)  # 120일 전의 타임스탬프
# data = collect_chart(1000, "ETHUSDT", "1d", gettimestamp)
# print(data.to_string(index=False))
# for i in range(len(data)):
#     data[i][0] = tsc.convert_unix(data[i][0])
#     data[i][6] = tsc.convert_unix(data[i][6])
# candles = convert_data(data)
#
# frame = pd.DataFrame(candles)
# frame.rename(columns={0: 'ts', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume', 6: 'te'},
#              inplace=True)
# frame['close'] = frame['close'].astype(float)
# frame['sma7'] = ta.trend.sma_indicator(frame['close'], window=7)
# frame['sma20'] = ta.trend.sma_indicator(frame['close'], window=20)
# frame['sma60'] = ta.trend.sma_indicator(frame['close'], window=60)
# frame['sma120'] = ta.trend.sma_indicator(frame['close'], window=120)
# frame['rsi'] = ta.momentum.RSIIndicator(close=frame['close'], window=14).rsi()
# frame['macd'] = ta.trend.macd(frame['close'])
# frame['signal'] = ta.trend.macd_signal(frame['close'])
#
# print(frame.to_string(index=False))
# 여기에 지표 분석 로직 추가

# if __name__ == "__main__":
#     asyncio.run(listen_binance_kline("btcusdt_perpetual", "1m"))
