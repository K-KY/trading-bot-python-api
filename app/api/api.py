import asyncio

from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.model.char_data_collector.chart_collector import listen_binance_kline
app = FastAPI()

@app.get("/collect")
async def get_chart(coin, interval):
    await listen_binance_kline("ethusdt_perpetual", "1m")
    return {"message": "Started websocket"}

