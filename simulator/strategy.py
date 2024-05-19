from dataclasses import dataclass
from pathlib import Path
from simulator.data_feeds import DataFeeds
from simulator.accounts import Accounts
from simulator.arbitrage_trade import FundingArbitrageTrade
import logging


@dataclass
class Config:
    init_cash: float
    margin_rate: float
    commission: float

    ordersize_usd: float
    fundrate_diff_open: float  # funding rate diff > this threshold, open trades
    fundrate_diff_close: float  # funding rate diff < this threshold, close trades

    data_dir: Path
    cex_list: list[str]
    symbol_list: list[str]


@dataclass
class ArbPair:
    symbol: str
    buy_cex: str
    sell_cex: str
    fundrate_diff: float


class FundingArbitrageStrategy:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._data_feeds = DataFeeds(
            data_dir=config.data_dir, cex_list=config.cex_list, symbol_list=config.symbol_list
        )

        self._cexs = {}
        for cex_name in config.cex_list:
            symbol_infos = {s: config.margin_rate for s in config.symbol_list}
            self._cexs[cex_name] = Accounts(
                name=cex_name,
                init_cash=config.init_cash / len(config.cex_list),
                symbol_infos=symbol_infos,
                commission=config.commission,
            )

        self._trades: list[FundingArbitrageTrade] = []

    def __best_arbpair_4symbol(self, symbol, funding_rates: dict[str, dict[str, float]]):
        max_frate_diff = 0
        buy_cex = None
        sell_cex = None
        for ii in range(len(self._config.cex_list)):
            cex1_name = self._config.cex_list[ii]
            fundrate1 = funding_rates[cex1_name][symbol]

            for jj in range(ii + 1, len(self._config.cex_list)):
                cex2_name = self._config.cex_list[jj]
                fundrate2 = funding_rates[cex2_name][symbol]

                frate_diff = abs(fundrate1 - fundrate2)
                if frate_diff > max_frate_diff:
                    buy_cex = cex1_name if fundrate1 < fundrate2 else cex2_name
                    sell_cex = cex2_name if fundrate1 < fundrate2 else cex1_name
                    max_frate_diff = frate_diff

        return ArbPair(symbol=symbol, buy_cex=buy_cex, sell_cex=sell_cex, fundrate_diff=max_frate_diff)

    def __is_holding(self, cex, symbol):
        for trade in self._trades:
            if not trade.is_active:
                continue
            
            buy_cex = trade.get_order('long').cex_name
            sell_cex = trade.get_order('short').cex_name
            
            if symbol == trade.symbol and (buy_cex == cex or sell_cex==cex):
                return True
            
        return False


    def open_trades(self, prices: dict[str, dict[str, float]], funding_rates: dict[str, dict[str, float]]):
        """
        Args:
            funding_rates (dict[str,dict[str,float]]): out-key=cex, inner dict: symbol->funding rate
        """
        for symbol in self._config.symbol_list:
            arbpair = self.__best_arbpair_4symbol(symbol=symbol, funding_rates=funding_rates)

            if arbpair.fundrate_diff < self._config.fundrate_diff_open:
                logging.info(
                    f"drop {arbpair} because its fundrate_diff < expected {self._config.fundrate_diff_open}"
                )
                continue

            # TODO: 这里是否还有改进的空间，一个cex可能在不同时间增加了多个对手方
            if self.__is_holding(cex=arbpair.buy_cex, symbol=symbol):
                continue

            if self.__is_holding(cex=arbpair.sell_cex, symbol=symbol):
                continue

            trade = FundingArbitrageTrade(
                symbol=symbol,
                long_cex=self._cexs[arbpair.buy_cex],
                short_cex=self._cexs[arbpair.sell_cex],
            )
            trade.open(
                usd_amount=self._config.ordersize_usd,
                prices={
                    "long": prices[arbpair.buy_cex][symbol],
                    "short": prices[arbpair.sell_cex][symbol],
                },
            )
            self._trades.append(trade)



    def close_trades(self, prices: dict[str, dict[str, float]], funding_rates: dict[str, dict[str, float]]):

        for trade in self._trades:
            if not trade.is_active:
                continue

            buy_cex = trade.get_order("long").cex_name
            buyside_frate = funding_rates[buy_cex][trade.symbol]

            sell_cex = trade.get_order("short").cex_name
            sellside_frate = funding_rates[sell_cex][trade.symbol]

            # TODO:目前只有一个终止退出的条件，就是发现套利机会消失，未来可以增加更多的止盈+止损条件
            if sellside_frate - buyside_frate < self._config.fundrate_diff_close:
                trade.close(
                    prices={
                        "long": prices[buy_cex][trade.symbol],
                        "short": prices[sell_cex][trade.symbol],
                    }
                )

    def run(self):
        for feed in self._data_feeds:
            self.open_trades()
