from simulator.data_feeds import DataFeeds, FeedOnce
from simulator.utils import hfr2a
from pprint import pprint
from prettytable import PrettyTable


class Tester:
    def __init__(self, exchanges: list[str], markets: list[str]) -> None:
        self.__exchanges = exchanges
        self.__markets = markets
        self.__data_feeds = DataFeeds(data_dir="data/input", exchanges=exchanges, markets=markets)
        self.__metric_names = ["open_price", "close_price", "mark_price", "fund_rate"]

    def display1(self, feed: FeedOnce, metric_name: str):
        all_values = feed.get(metric_name)

        title = "Annual Funding Rate" if metric_name == "fund_rate" else metric_name
        pt = PrettyTable(["market"] + self.__exchanges, title=title)
        for market in self.__markets:
            ex_values = all_values[market]  # 同一个market不同exchange的数值

            if metric_name != "fund_rate":
                values = [f"{ex_values[e]:.2f}" for e in self.__exchanges]
            else:
                values = [f"{hfr2a(ex_values[e]):.2%}" for e in self.__exchanges]

            pt.add_row([market] + values)

        print(pt)

    def __display_by_market(self, pt: PrettyTable, market: str, feed: FeedOnce):
        for eidx, exchange in enumerate(self.__exchanges):
            row = [market if eidx == 0 else "", exchange]
            for metric_name in self.__metric_names:
                ex_values = feed.get(metric_name)[market]
                value = ex_values[exchange]
                if metric_name == "fund_rate":
                    val_txt = f"{hfr2a(value):.2%}"
                else:
                    val_txt = f"{value:.2f}"
                row.append(val_txt)
            pt.add_row(row)

    def run(self):
        for tidx, feed in enumerate(self.__data_feeds, start=1):
            print(f"\n******************* [{tidx:02d}] {feed.timestamp.strftime('%Y-%m-%d %H:%M:%SZ')}")

            pt = PrettyTable(["market", "exchange"] + self.__metric_names)
            for midx, market in enumerate(self.__markets, start=1):
                self.__display_by_market(pt=pt, market=market, feed=feed)

                if midx < len(self.__markets):
                    pt.add_row(["---------"] * (2 + len(self.__metric_names)))

            print(pt)


def test():
    coins = ["btc", "eth", "sol"]
    tester = Tester(exchanges=["dydx", "rabbitx"], markets=[c.upper() + "-USD" for c in coins])
    tester.run()


if __name__ == "__main__":
    test()
