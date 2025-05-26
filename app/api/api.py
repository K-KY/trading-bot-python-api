import subprocess
import uvicorn

from app.model.char_data_collector.chart_collector import (save_data, get_time_stamp_range, collect_chart)
from app.model.char_data_collector.thread_manager import ThreadManager

from fastapi import FastAPI

from app.api_test import api_test


app = FastAPI()
app.include_router(api_test.router, prefix='/api')

thread_manager = ThreadManager()

@app.get("/")
def test():
    return "OK"

# 차트 웹소켓 스레드
@app.get("/start-task")
async def start_background_chart_monitoring(coin, interval):
    thread_manager.run_monitor_chart(coin, interval)
    return {"status": "작업이 백그라운드에서 시작되었습니다."}

#실행중인 스레드 목록
@app.get("/workers")
def workers():
    return thread_manager.get_all_workers()

@app.get("/stop-worker")# 종료 요청하고 바로 다시 요청을 보냈을 때 종료 됨으로 반환됨
def stop_worker(name:str) :
    return thread_manager.stop_thread(name)

@app.get("/save-chart")
def save_chart(limit: int, coin: str, interval: str) :
    candles = collect_chart(limit, coin, interval)
    save_data(candles, coin, interval)

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
