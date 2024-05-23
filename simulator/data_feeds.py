import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict


class FeedOnce:  # 某个时刻下的数据
    def __init__(self) -> None:
        self.timestamp: datetime = None
        self.is_last: bool = None
        
        # 外层key是market，内层dict是exchange -> price / funding rate
        self.open_prices: dict[str, dict[str, float]] = defaultdict(dict)
        self.close_prices: dict[str, dict[str, float]] = defaultdict(dict)
        self.mark_prices: dict[str, dict[str, float]] = defaultdict(dict)
        self.funding_rates: dict[str, dict[str, float]] = defaultdict(dict)
        
        self.__col2container = {
            "open_price": self.open_prices,
            "close_price": self.close_prices,
            "mark_price": self.mark_prices,
            "fund_rate": self.funding_rates,
        }

    def add(self, column: str, market: str, exchange: str, value: float):
        container = self.__col2container[column]
        # 外层key是market，内层dict是exchange -> value
        container[market][exchange] = value


class DataFeeds:
    def __init__(self, data_dir: Path, exchanges: list[str], markets: list[str]) -> None:
        self._datas = {}
        self._exchanges = exchanges
        self._markets = markets
        self._index = 0

        self._total_rows = None
        for ex in exchanges:
            for market in markets:

                fname = data_dir / f"{ex}_{market}.csv"
                df = pd.read_csv(fname, index_col="timestamp", parse_dates=True)

                if self._total_rows is None:
                    self._total_rows = df.shape[0]
                else:
                    assert self._total_rows == df.shape[0], "all exchanges should have same length of data"

                self._datas[f"{ex}/{market}"] = df

    def __iter__(self):
        return self

    def __read_current(self):
        feed = FeedOnce()

        for market in self._markets:
            for ex in self._exchanges:
                row = self._datas[f"{ex}/{market}"].iloc[self._index, :]

                # TODO:只要存在NaN，就放弃这个timestamp的所有market+exchange的数据
                if row.isna().any():
                    return None

                if feed.timestamp is None:
                    feed.timestamp = row.name.to_pydatetime()
                else:
                    assert feed.timestamp == row.name.to_pydatetime()

                for col in ["open_price", "close_price", "mark_price", "fund_rate"]:
                    feed.add(column=col, market=market, exchange=ex, value=row[col])

        return feed

    def __next__(self):
        while self._index < self._total_rows:
            feed = self.__read_current()
            feed.is_last = self._index == (self._total_rows - 1)

            self._index += 1

            if feed is not None:
                return feed

        raise StopIteration
