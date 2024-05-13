import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime


class FeedOnce:
    def __init__(self) -> None:
        self.timestamp: datetime = None
        self.prices: dict[str, dict[str, float]] = {}
        self.funding_rates: dict[str, dict[str, float]] = {}


class DataFeeds:
    def __init__(self, data_dir: Path, cex_list: list[str], symbol_list: list[str]) -> None:
        self._datas = {}
        self._symbol_list = symbol_list
        self._index = 0
        
        self._total_rows = None
        for cex in cex_list:
            fname = data_dir / f"{cex}.csv"
            df = pd.read_csv(fname, index_col="timestamp", parse_dates=True)

            if self._total_rows is None:
                self._total_rows = df.shape[0]
            else:
                assert self._total_rows == df.shape[0], "all CEX should have same length of data"

            self._datas[cex] = df

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= self._total_rows:
            raise StopIteration

        feed = FeedOnce()

        for cex, df in self._datas.items():
            row = df.iloc[self._index, :]

            if feed.timestamp is None:
                feed.timestamp = row.name.to_pydatetime()
            else:
                assert feed.timestamp == row.name.to_pydatetime()

            feed.prices[cex] = {symbol: row[symbol + "_price"] for symbol in self._symbol_list}
            feed.funding_rates[cex] = {symbol: row[symbol + "_fundrate"] for symbol in self._symbol_list}

        self._index += 1

        return feed
