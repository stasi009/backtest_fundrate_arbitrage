import asyncio
import httpx
import pandas as pd
from datetime import timezone, datetime, timedelta, time
import typer
from prepare.common import UTC_TM_FORMAT, truncate_to_hour, check_http_error, safe_output_path


class DownloaderBase:
    def __init__(self, market: str, data_type: str) -> None:
        self.market = market
        self.data_type = data_type

    def _url(self) -> str:
        raise NotImplementedError()

    def _params(self, end_time: datetime) -> dict:
        raise NotImplementedError()

    def _parse(self, json: dict):
        raise NotImplementedError()

    async def download(self, start_time: datetime, end_time: datetime):
        url = self._url()

        all_results = []
        async with httpx.AsyncClient() as client:
            while end_time >= start_time:
                response = await client.get(url, params=self._params(end_time))
                check_http_error(response)

                results = self._parse(response.json())
                if len(results) == 0:
                    break

                all_results.extend(results)

                # 按时间倒序存放每小时的funding rate，最后一行才是最老的
                first_time = results[-1]["timestamp"]
                print(
                    f"downloaded DYDX[{self.market}] {len(results)} {self.data_type} {first_time} ~ {end_time}"
                )

                await asyncio.sleep(1)
                end_time = first_time - timedelta(seconds=1)

        df = pd.DataFrame(all_results)
        df.set_index("timestamp", inplace=True)
        return df


class FundRateDownloader(DownloaderBase):
    def __init__(self, market: str) -> None:
        super().__init__(market, "FundRate")

    def _url(self) -> str:
        return f"https://api.dydx.exchange/v3/historical-funding/{self.market}"

    def _params(self, end_time: datetime) -> dict:
        return {"effectiveBeforeOrAt": end_time.strftime(UTC_TM_FORMAT)}

    def _parse(self, json: dict):
        return [
            {
                "timestamp": truncate_to_hour(r["effectiveAt"]),
                "fund_rate": r["rate"],
                # https://dydxprotocol.github.io/v3-teacher/#funding-payment-calculation
                # 所谓的price，即oracle price，参与计算funding payment
                "mark_price": r["price"],
            }
            for r in json["historicalFunding"]
        ]


class CandleDownloader(DownloaderBase):
    def __init__(self, market: str) -> None:
        super().__init__(market, "Candle")

    def _url(self) -> str:
        return f"https://api.dydx.exchange/v3/candles/{self.market}"

    def _params(self, end_time: datetime) -> dict:
        return {"toISO": end_time.strftime(UTC_TM_FORMAT), "resolution": "1HOUR"}

    def _parse(self, json: dict):
        return [
            {
                "timestamp": truncate_to_hour(r["startedAt"]),
                "open_price": r["open"],
                "close_price": r["close"],
            }
            for r in json["candles"]
        ]


async def __main__(market: str, start_time: datetime, end_time: datetime):
    fundrate_downloader = FundRateDownloader(market)
    df_fundrates = await fundrate_downloader.download(start_time=start_time, end_time=end_time)

    candle_downloader = CandleDownloader(market)
    df_candles = await candle_downloader.download(start_time=start_time, end_time=end_time)

    df = df_fundrates.join(df_candles, how="outer")
    df.sort_index(inplace=True)

    outfname = safe_output_path(f"data/raw/dydx_{market}.csv")
    df.to_csv(outfname, index_label="timestamp", date_format="%Y-%m-%d %H:%M:%S")


def main(market: str, start_day: datetime, end_day: datetime):
    end_time = datetime.combine(end_day.date(), time(23, 59, 59))  # 终止那天的最后一个小时
    start_time = datetime.combine(start_day.date(), time())  # 开始那天的第一个小时

    start_time = start_time.replace(tzinfo=timezone.utc)
    end_time = end_time.replace(tzinfo=timezone.utc)

    asyncio.run(__main__(market=market, start_time=start_time, end_time=end_time))


if __name__ == "__main__":
    typer.run(main)
