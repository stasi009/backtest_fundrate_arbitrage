import asyncio
import httpx
import pandas as pd
from datetime import timezone, datetime, timedelta, time
import typer
from common import UTC_TM_FORMAT, truncate_to_hour, check_http_error, raw_data_path
import logging


async def download_fundrates(market: str, start_time: datetime, end_time: datetime):
    # 别用urljoin，特别难用，api设计非常反直觉
    url = f"https://api.dydx.exchange/v3/historical-funding/{market}"

    all_results = []
    async with httpx.AsyncClient() as client:
        while end_time >= start_time:
            response = await client.get(
                url, params={"effectiveBeforeOrAt": end_time.strftime(UTC_TM_FORMAT)}
            )
            check_http_error(response)

            results = response.json()["historicalFunding"]
            if len(results) == 0:
                break

            results = [
                {
                    "effectiveAt": truncate_to_hour(r["effectiveAt"]),
                    "fund_rate": r["rate"],
                    "mark_price": r["price"],
                }
                for r in results
            ]
            all_results.extend(results)

            # 按时间倒序存放每小时的funding rate，最后一行才是最老的
            first_time = results[-1]["effectiveAt"]
            print(f"downloaded DYDX[{market}] {len(results)} funding rate {first_time} ~ {end_time}")

            await asyncio.sleep(1)
            end_time = first_time - timedelta(seconds=1)

    return pd.DataFrame(all_results)


def main(market: str, start_day: datetime, end_day: datetime):
    end_time = datetime.combine(end_day.date(), time(23, 0, 0))  # 终止那天的最后一个小时
    start_time = datetime.combine(start_day.date(), time())  # 开始那天的第一个小时

    start_time = start_time.replace(tzinfo=timezone.utc)
    end_time = end_time.replace(tzinfo=timezone.utc)

    asyncio.run(download_fundrates(market=market, start_time=start_time, end_time=end_time))


if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    typer.run(main)
