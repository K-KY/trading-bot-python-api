import asyncio
import websockets
import json
import ssl
import threading

import app.util.TimeStampConverter as tsc

BASE_URL = "https://www.binance.com/fapi/v1/continuousKlines"
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams={}@continuousKline_{}"


class ThreadManager():
    stop_event = {}
    threads = {}

    def __init__(self):
        self.stop_event = {}
        self.threads = {}

    @staticmethod
    def runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

    # 존재하지 않는 코인 검증
    @staticmethod
    async def is_valid_symbol(websocket, name: str):
        try:
            await asyncio.wait_for(websocket.recv(), timeout=5.0)
            return True  # 또는 True 반환도 가능

        except asyncio.TimeoutError:
            print(f"[{name}] 웹소켓 데이터 수신 시간 초과")
            return False  # 또는 False

    # 쓰레드 생성, 작업 할당
    def run_monitor_chart(self, coin: str, interval: str):
        name = "thread-worker_" + str(len(self.threads) + 1) + "__coin__" + coin + "__interval__" + interval

        # timestamp = get_time_stamp_range(interval, 300)
        # save_data(300, coin, interval, timestamp)

        event = threading.Event()
        thread = threading.Thread(
            target=self.__run_listen_kline,
            args=(coin, interval, name),
            name=name,
            daemon=True
        )
        self.threads[name] = thread
        self.stop_event[name] = event
        thread.start()

    ## 쓰레드 개별 종료 로직 개선의 여지가 있음
    def stop_thread(self, thread_name: str):
        if thread_name in self.stop_event and not self.threads[thread_name].is_alive():
            return thread_name + " 이미 종료됨"
        if thread_name in self.stop_event:
            self.stop_event[thread_name].set()  # 쓰레드 종료 신호
            print(f"[{thread_name}] 종료 요청됨")
            return thread_name + " 종료 요청 완료"
        return thread_name + " 찾을 수 없음"

    # 쓰레드가 수행할 작업
    def __run_listen_kline(self, coin: str, interval: str, name: str):
        loop = self.runner()
        loop.run_until_complete(self.listen_binance_kline(coin, interval, name))

    # binance 웹소켓 연결, 여기에 지표 추가
    async def listen_binance_kline(self, coin: str, interval: str, name: str):
        ssl_context = ssl._create_unverified_context()
        url = BINANCE_WS_URL.format(coin, interval)
        async with websockets.connect(url, ssl=ssl_context) as websocket:
            if not await self.is_valid_symbol(websocket, name):
                self.stop_thread(name)

            print(f"WebSocket 연결됨! ({coin}, {interval})")
            while not self.stop_event[name].is_set():
                data = await websocket.recv()
                parsed = json.loads(data)
                kline = parsed['data']['k']
                kline['t'] = tsc.convert_unix(kline['t'])
                kline['T'] = tsc.convert_unix(kline['T'])
                # save_live_data(coin, kline)
                print(kline)
            self.stop_event[name].is_set()

    ## 작업중인 쓰레드 반환
    def get_all_workers(self):
        return [
            {
                "name": name,
                "alive": t.is_alive(),
                "ident": t.ident
            }
            for name, t in self.threads.items()
        ]
