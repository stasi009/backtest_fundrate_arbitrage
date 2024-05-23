import asyncio
import httpx
import pandas as pd
from datetime import timezone, datetime, time
import typer
from prepare.common import safe_output_path, check_http_error

MICRO_PER_SECOND = 1000000
SLEEP_SECONDS = 0.3


def microsec_to_datetime(micro_seconds: int) -> datetime:
    return datetime.fromtimestamp(micro_seconds / MICRO_PER_SECOND, tz=timezone.utc)


def datetime_to_microsec(local_dt: datetime) -> int:
    # 输入的dt只是local datetime，必须转化成UTC datetime
    return int(local_dt.replace(tzinfo=timezone.utc).timestamp()) * MICRO_PER_SECOND


class DownloaderBase:
    def __init__(self, market: str, data_type: str) -> None:
        self.market = market
        self.data_type = data_type

    def _url(self) -> str:
        raise NotImplementedError()

    def _params(self, start_micro_sec: int, end_micro_sec: int) -> dict:
        raise NotImplementedError()

    def _parse(self, json: dict):
        raise NotImplementedError()

    async def download(self, start_time: datetime, end_time: datetime):
        start_micro_sec = datetime_to_microsec(start_time)
        end_micro_sec = datetime_to_microsec(end_time)

        all_results = []
        async with httpx.AsyncClient() as client:
            while start_micro_sec < end_micro_sec:
                params = self._params(start_micro_sec=start_micro_sec, end_micro_sec=end_micro_sec)
                response = await client.get(self._url(), params=params)
                check_http_error(response)

                batch_results = self._parse(response.json())
                if len(batch_results) == 0:
                    break
                all_results.extend(batch_results)

                print(
                    f"downloaded Rabbitx[{self.market}] {len(batch_results)} "
                    f"{self.data_type} {batch_results[0]['timestamp']} ~ {batch_results[-1]['timestamp']}"
                )

                await asyncio.sleep(SLEEP_SECONDS)
                start_micro_sec = datetime_to_microsec(batch_results[-1]["timestamp"]) + MICRO_PER_SECOND

        df = pd.DataFrame(all_results)
        df.set_index("timestamp", inplace=True)
        return df


class FundRateDownloader(DownloaderBase):
    def __init__(self, market: str) -> None:
        super().__init__(market, "FundRate")

    def _url(self) -> str:
        return "https://api.prod.rabbitx.io/markets/fundingrate"

    def _params(self, start_micro_sec: int, end_micro_sec: int) -> dict:
        return {"market_id": self.market, "p_limit": 100, "start_time": start_micro_sec, "p_order": "ASC"}

    def _parse(self, json: dict):
        batch_results = []

        for r in json["result"]:
            rate = float(r["funding_rate"])
            dt = microsec_to_datetime(r["timestamp"])
            # 防止有误差导致小时之外还有数字
            effectiveAt = dt.replace(minute=0, second=0, microsecond=0)
            batch_results.append({"timestamp": effectiveAt, "fund_rate": rate})

        return batch_results


async def __main__(market: str, start_time: datetime, end_time: datetime):
    fundrate_downloader = FundRateDownloader(market)
    df_fundrates = await fundrate_downloader.download(start_time=start_time, end_time=end_time)

    # await asyncio.sleep(SLEEP_SECONDS)

    # candle_downloader = CandleDownloader(market)
    # df_candles = await candle_downloader.download(start_time=start_time, end_time=end_time)

    # df = df_fundrates.join(df_candles, how="outer")
    # df.sort_index(inplace=True)

    df = df_fundrates

    outfname = safe_output_path(f"data/raw/rabbitx_{market}.csv")
    df.to_csv(outfname, index_label="timestamp", date_format="%Y-%m-%d %H:%M:%S")


def main(market: str, start_day: datetime, end_day: datetime):
    end_time = datetime.combine(end_day.date(), time(23, 59, 59))  # 终止那天的最后一秒
    start_time = datetime.combine(start_day.date(), time())  # 开始那天的第一秒
    asyncio.run(__main__(market=market, start_time=start_time, end_time=end_time))


if __name__ == "__main__":
    typer.run(main)
