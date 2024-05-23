import asyncio
import httpx
import pandas as pd
from datetime import timezone, datetime, timedelta, time
import typer
from common import UTC_TM_FORMAT, raw_data_path, check_http_error
import logging

URL = "https://api.prod.rabbitx.io/markets/fundingrate"
MICRO_PER_SECOND = 1000000


def microsec_to_datetime(micro_seconds: int) -> datetime:
    return datetime.fromtimestamp(micro_seconds / MICRO_PER_SECOND, tz=timezone.utc)


def datetime_to_microsec(local_dt: datetime) -> int:
    # 输入的dt只是local datetime，必须转化成UTC datetime
    return int(local_dt.replace(tzinfo=timezone.utc).timestamp()) * MICRO_PER_SECOND


async def __main__(market: str, start_time: datetime, end_time: datetime):
    start_micro_sec = datetime_to_microsec(start_time)
    end_micro_sec = datetime_to_microsec(end_time)

    all_results = []
    async with httpx.AsyncClient() as client:
        while start_micro_sec < end_micro_sec:
            params = {"market_id": market, "p_limit": 100, "start_time": start_micro_sec, "p_order": "ASC"}

            response = await client.get(URL, params=params)
            check_http_error(response)
            batch_results = response.json()["result"]
            if len(batch_results) == 0:
                break

            for r in batch_results:
                rate = float(r["funding_rate"])
                dt = microsec_to_datetime(r["timestamp"])
                # 防止有误差导致小时之外还有数字
                effectiveAt = dt.replace(minute=0, second=0, microsecond=0)
                all_results.append({"rate": rate, "effectiveAt": effectiveAt})

            print(
                f"downloaded Rabbitx[{market}] {len(batch_results)} "
                f"funding rate {microsec_to_datetime(batch_results[0]['timestamp'])} "
                f"~ {microsec_to_datetime(batch_results[-1]['timestamp'])}"
            )

            await asyncio.sleep(1)
            start_micro_sec = batch_results[-1]["timestamp"] + MICRO_PER_SECOND

    # all columns: market,rate,price,effectiveAt
    df = pd.DataFrame(all_results)
    data_path = raw_data_path(exchange="rabbitx", market=market)
    df.to_csv(data_path, index=False, date_format="%Y-%m-%d %H:%M:%S")


def main(market: str, start_day: datetime, end_day: datetime):
    end_time = datetime.combine(end_day.date(), time(23, 0, 0))  # 终止那天的最后一个小时
    start_time = datetime.combine(start_day.date(), time())  # 开始那天的第一个小时
    asyncio.run(__main__(market=market, start_time=start_time, end_time=end_time))


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    typer.run(main)
