from dataclasses import dataclass
from pathlib import Path
from simulator.data_feeds import DataFeeds
from simulator.cex_accounts import CexAccounts
from simulator.arbitrage_trade import FundingArbitrageTrade
import logging


@dataclass
class Config:
    init_cash: float
    margin_rate: float
    commission: float
    min_fundrate_diff: float

    data_dir: Path
    cex_list: list[str]
    symbol_list: list[str]


@dataclass
class ArbPair:
    symbol: str
    buy_cex: str
    sell_cex: str
    fundrate_diff: float


class Strategy:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._data_feeds = DataFeeds(
            data_dir=config.data_dir, cex_list=config.cex_list, symbol_list=config.symbol_list
        )

        self._cexs = {}
        for cex_name in config.cex_list:
            symbol_infos = {s: config.margin_rate for s in config.symbol_list}
            self._cexs[cex_name] = CexAccounts(
                name=cex_name,
                init_cash=config.init_cash / len(config.cex_list),
                symbol_infos=symbol_infos,
                commission=config.commission,
            )

        self._trades = []
        self._holding_contracts = set()

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
        return (symbol + "@" + cex) in self._holding_contracts

    def __hold(self, cex, symbol):
        self._holding_contracts.add(symbol + "@" + cex)

    def __unhold(self, cex, symbol):
        self._holding_contracts.remove(symbol + "@" + cex)

    def open_trades(self, funding_rates: dict[str, dict[str, float]]):
        """
        Args:
            funding_rates (dict[str,dict[str,float]]): out-key=cex, inner dict: symbol->funding rate
        """
        for symbol in self._config.symbol_list:
            arbpair = self.__best_arbpair_4symbol(symbol=symbol, funding_rates=funding_rates)

            if arbpair.fundrate_diff < self._config.min_fundrate_diff:
                logging.info(f"drop {arbpair} because its fundrate_diff < expected {self._config.min_fundrate_diff}")
                continue

            # TODO: 这里是否还有改进的空间，一个cex可能在不同时间增加了多个对方手
            if self.__is_holding(cex=arbpair.buy_cex, symbol=symbol):
                continue

            if self.__is_holding(cex=arbpair.sell_cex, symbol=symbol):
                continue

            trade = FundingArbitrageTrade(
                contract=symbol,
                long_cex=self._cexs[arbpair.buy_cex],
                short_cex=self._cexs[arbpair.sell_cex],
            )
            self._trades.append(trade)
            trade.open()

    def run(self):
        for feed in self._data_feeds:
            pass
