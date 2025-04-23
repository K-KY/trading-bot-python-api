import requests
import time
import asyncio
import websockets
import json
import ssl
import threading

BASE_URL = "https://www.binance.com/fapi/v1/continuousKlines"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"

stop_event = {}
threads = {}

# 개별 쓰레드를 종료시킬 방법 생각

#쓰레드 생성, 작업 할당
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

#쓰레드가 수행할 작업
def __run_listen_kline(coin: str, interval: str, name):
    loop = runner()
    loop.run_until_complete(listen_binance_kline(coin, interval, name))

# binance 웹소켓 연결, 여기에 지표 추가
async def listen_binance_kline(coin: str, interval: str, name:str):
    ssl_context = ssl._create_unverified_context()
    url = BINANCE_WS_URL.format(coin, interval)
    async with websockets.connect(url, ssl=ssl_context) as websocket:
        print(f"WebSocket 연결됨! ({coin}, {interval})")
        while not stop_event[name].is_set() :
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

## 쓰레드 개별 종료 로직 개선의 여지가 있음
def stop_thread(thread_name:str):
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
# print(data)
# 여기에 지표 분석 로직 추가

# data = collect_chart(100, "ETHUSDT", "1d", gettimestamp)
# print(data)
#
# if __name__ == "__main__":
#     asyncio.run(listen_binance_kline("btcusdt_perpetual", "1m"))
