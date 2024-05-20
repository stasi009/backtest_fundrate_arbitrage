from dataclasses import dataclass
from datetime import datetime
from simulator.data_feeds import DataFeeds
from simulator.exchange import Exchange
from simulator.arbitrage_trade import FundingArbTrade
from simulator.config import Config


@dataclass
class ArbPair:
    market: str
    long_ex: str
    short_ex: str
    fundrate_diff: float


class FundingArbStrategy:
    def __init__(self, config: Config) -> None:
        self._config = config

        self._data_feeds = DataFeeds(
            data_dir=config.data_dir, exchanges=config.exchanges, markets=config.markets
        )

        self._exchanges = {
            ex_name: Exchange(
                name=ex_name,
                init_cash=config.init_cash / len(config.exchanges),
                markets={m: config.margin_rate for m in config.markets},
                commission=config.commission,
            )
            for ex_name in config.exchanges
        }

        self._active_arb_trades: dict[str, FundingArbTrade] = {}  # market --> trade
        self._closed_trades: list[FundingArbTrade] = {}

    def _best_arb_pair(self, market: str, funding_rates: dict[str, dict[str, float]]) -> ArbPair:
        """
        Args:
            funding_rates: out-key=market, inner dict: exchange->price
        """
        ex_fundrates = funding_rates[market]

        max_frate_diff = 0
        long_ex = None
        short_ex = None
        for ii in range(len(self._config.exchanges)):
            ex1_name = self._config.exchanges[ii]
            fundrate1 = ex_fundrates[ex1_name]

            for jj in range(ii + 1, len(self._config.exchanges)):
                ex2_name = self._config.exchanges[jj]
                fundrate2 = ex_fundrates[ex2_name]

                frate_diff = abs(fundrate1 - fundrate2)
                if frate_diff > max_frate_diff:
                    long_ex = ex1_name if fundrate1 < fundrate2 else ex2_name
                    short_ex = ex2_name if fundrate1 < fundrate2 else ex1_name
                    max_frate_diff = frate_diff

        return ArbPair(market=market, long_ex=long_ex, short_ex=short_ex, fundrate_diff=max_frate_diff)

    def __open(self, arbpair: ArbPair):
        """返回两个trade，第1个是要关闭的trade，第2个是要开仓或加仓的trade"""
        new_trade = FundingArbTrade(
            market=arbpair.market,
            long_ex=self._exchanges[arbpair.long_ex],
            short_ex=self._exchanges[arbpair.short_ex],
        )

        if arbpair.market not in self._active_arb_trades:
            self._active_arb_trades[arbpair.market] = new_trade
            print(f"open new trade: {new_trade.name}")
            return None, new_trade

        old_trade = self._active_arb_trades[arbpair.market]

        if (old_trade.name == new_trade.name) and (
            arbpair.fundrate_diff
            >= old_trade.open_fundrate_diff * (1 + self._config.fundrate_diff_change_pct)
        ):
            print(f"increase position on {new_trade.name}")
            return None, old_trade  # 加仓

        if (old_trade.name != new_trade.name) and (
            arbpair.fundrate_diff
            >= old_trade.latest_fundrate_diff * (1 + self._config.fundrate_diff_change_pct)
        ):
            print(f"change trade from {old_trade.name} to {new_trade.name}")
            # 关闭active_trade，开仓new_trade
            return old_trade, new_trade

        return None, None

    def open(
        self, tm: datetime, prices: dict[str, dict[str, float]], funding_rates: dict[str, dict[str, float]]
    ):
        """
        Args:
            prices:        out-key=market, inner dict: exchange->price
            funding_rates: out-key=market, inner dict: exchange->funding rate
        """
        for market in self._config.markets:
            arbpair = self._best_arb_pair(ex_fundrates=funding_rates[market])
            if arbpair.fundrate_diff < self._config.fundrate_diff_open:
                continue

            trade2close, trade2open = self.__open(arbpair)
            if trade2close is not None:
                trade2close.close(tm, prices[market])

            if trade2open is not None:
                trade2open.open(
                    tm=tm,
                    usd_amount=self._config.ordersize_usd,
                    prices=prices[market],
                    fundrate_diff=arbpair.fundrate_diff,
                )

    def close(self, prices: dict[str, dict[str, float]], funding_rates: dict[str, dict[str, float]]):

        for trade in self._trades:
            if not trade.is_active:
                continue

            buy_cex = trade.get_order("long").ex_name
            buyside_frate = funding_rates[buy_cex][trade.market]

            sell_cex = trade.get_order("short").ex_name
            sellside_frate = funding_rates[sell_cex][trade.market]

            # TODO:目前只有一个终止退出的条件，就是发现套利机会消失，未来可以增加更多的止盈+止损条件
            if sellside_frate - buyside_frate < self._config.fundrate_diff_close:
                trade.close(
                    prices={
                        "long": prices[buy_cex][trade.market],
                        "short": prices[sell_cex][trade.market],
                    }
                )

    def run(self):
        for feed in self._data_feeds:
            self.open_trades()
