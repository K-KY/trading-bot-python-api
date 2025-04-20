import subprocess
import threading
import uvicorn

from app.model.char_data_collector.chart_collector import listen_binance_kline
from app.model.char_data_collector.chart_collector import run_listen_kline

from fastapi import FastAPI

from app.api_test import api_test

stop_event = threading.Event()
threads = {}

app = FastAPI()
app.include_router(api_test.router, prefix='/api')


@app.get("/")
def test():
    return "OK"

# 단일 차트 웹소켓
@app.get("/connect-wss")
async def get_chart(coin, interval):
    await listen_binance_kline(coin, interval)
    return {"message": "Started websocket"}


# 차트 웹소켓 스레드
@app.get("/start-task")
async def start_background_chart_monitoring(coin, interval):
    name = "thread-worker " + str(len(threads) + 1) + ",coin = " + coin + ",interval = " + interval
    thread = threading.Thread(target=run_listen_kline, args=(coin, interval),
                              name=name, daemon=True)
    threads[name] = thread
    thread.start()
    return {"status": "작업이 백그라운드에서 시작되었습니다."}

# 실행중인 스레드 목록
@app.get("/workers")
def workers():
    return [
        {
            "name": name,
            "alive": t.is_alive(),
            "ident": t.ident
        }
        for name, t in threads.items()
    ]


if __name__ == "__main__":
    # 포트 8000에서 실행 중인 프로세스 ID 찾기

    result = subprocess.run(["lsof", "-t", "-i", ":8000"], stdout=subprocess.PIPE, text=True)

    # 프로세스 ID가 있을 경우 종료 명령 실행
    if result.stdout.strip():
        pid = result.stdout.strip()  # 프로세스 ID를 가져옵니다.
        subprocess.run(["kill", "-9", pid])
        print(f"포트 8000에서 실행 중인 프로세스 {pid}를 종료했습니다.")
    else:
        print("포트 8000에서 실행 중인 프로세스가 없습니다.")

    # FastAPI 서버 실행
    uvicorn.run("app.api.api:app", reload=True)
