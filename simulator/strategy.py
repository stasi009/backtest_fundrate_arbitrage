from dataclasses import dataclass
from typing import Tuple
from datetime import datetime
from simulator.data_feeds import DataFeeds, FeedOnce
from simulator.exchange import Exchange
from simulator.arbitrage_trade import FundingArbTrade
from simulator.utils import Config, hfr2a
import logging


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

        # market --> trade，同一时刻一个market只存在一个trade，1 long vs. 1 short，不存在multi long vs. multi short可能性
        self._active_arb_trades: dict[str, FundingArbTrade] = {}
        self.closed_trades: list[FundingArbTrade] = []

    def iter_exchanges(self):
        return self._exchanges.values()

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
                    # 如果两个fundrate都正，在fundrate更小的ex long，支付较少funding，在fundrate更大的ex short，收取较多的funding
                    # 如果两个fundrate都负，在fundrate更负的ex long，收取较多funding，在abs(fundrate)小的ex short，支付较少funding
                    # 如果两个fundrate一正一负，在fundrate<0的ex long，收取funding，在fundrate>0的ex short，收取funding
                    long_ex = ex1_name if fundrate1 < fundrate2 else ex2_name
                    short_ex = ex2_name if fundrate1 < fundrate2 else ex1_name
                    max_frate_diff = frate_diff

        logging.debug(
            f"[{market}] best pair: "
            f"long {long_ex} with AFR={hfr2a(ex_fundrates[long_ex]):.2%}, "
            f"short {short_ex} with AFR={hfr2a(ex_fundrates[short_ex]):.2%}, "
            f"AFRdiff={hfr2a(max_frate_diff):.2%}"
        )
        return ArbPair(market=market, long_ex=long_ex, short_ex=short_ex, fundrate_diff=max_frate_diff)

    def __open(self, arbpair: ArbPair) -> Tuple[FundingArbTrade, FundingArbTrade]:
        """返回两个trade，第1个是要关闭的trade，第2个是要开仓或加仓的trade"""
        new_trade = FundingArbTrade(
            market=arbpair.market,
            long_ex=self._exchanges[arbpair.long_ex],
            short_ex=self._exchanges[arbpair.short_ex],
            config=self._config,
        )

        if arbpair.market not in self._active_arb_trades:
            self._active_arb_trades[arbpair.market] = new_trade
            logging.info(
                f"open new trade: {new_trade.name}, when AFRdiff={hfr2a(arbpair.fundrate_diff):.2%}"
            )
            return None, new_trade

        old_trade = self._active_arb_trades[arbpair.market]

        # 本次发现的best pair与上次发现的best pair相同，而且fundrate_diff进一步扩大，加仓
        if (old_trade.name == new_trade.name) and (
            arbpair.fundrate_diff
            >= old_trade.open_fundrate_diff * (1 + self._config.fundrate_diff_change_pct)
        ):
            logging.info(f"increase position on {new_trade.name}")
            return None, old_trade  # 加仓

        # 本次发现的best pair与上次发现的best pair不同，并且fundrate_diff大了很多，换仓
        if (old_trade.name != new_trade.name) and (
            arbpair.fundrate_diff
            >= old_trade.latest_fundrate_diff * (1 + self._config.fundrate_diff_change_pct)
        ):
            logging.info(f"change trade from {old_trade.name} to {new_trade.name}")
            # 关闭old active trade，开仓new_trade
            return old_trade, new_trade

        return None, None

    def __close(self, trade: FundingArbTrade, tm: datetime, ex2prices: dict[str, float]):
        trade.close(tm, ex2prices)
        self.closed_trades.append(trade)

    def open(
        self,
        tm: datetime,
        prices: dict[str, dict[str, float]],
        funding_rates: dict[str, dict[str, float]],
    ):
        """
        Args:
            prices:        out-key=market, inner dict: exchange->price
            funding_rates: out-key=market, inner dict: exchange->funding rate
        """
        for market in self._config.markets:
            arbpair = self._best_arb_pair(market=market, funding_rates=funding_rates)
            if arbpair.fundrate_diff < self._config.fundrate_diff_open:  # fundingrate差异不够大
                continue

            trade2close, trade2open = self.__open(arbpair)
            if trade2close is not None:
                self.__close(trade2close, tm, prices[market])

            if trade2open is not None:
                trade2open.safe_open(
                    tm=tm,
                    usd_amount=self._config.ordersize_usd,
                    ex2prices=prices[market],
                    fundrate_diff=arbpair.fundrate_diff,
                )

    def close(
        self,
        tm: datetime,
        prices: dict[str, dict[str, float]],
        funding_rates: dict[str, dict[str, float]],
    ):
        """
        Args:
            prices:        out-key=market, inner dict: exchange->price
            funding_rates: out-key=market, inner dict: exchange->funding rate
        """
        keep_open_trades = {}
        for market, trade in self._active_arb_trades.items():
            trade.diff_fundrates(funding_rates[market])

            if trade.latest_fundrate_diff < self._config.fundrate_diff_close:  # fundrate差异收窄
                self.__close(trade, tm, prices[market])
            else:
                keep_open_trades[market] = trade
        self._active_arb_trades = keep_open_trades

    def run(self):
        for idx, feed in enumerate(self._data_feeds, start=1):
            logging.info(f"\n********************** [{idx}] {feed.timestamp}")
            self.close(tm=feed.timestamp, prices=feed.open_prices, funding_rates=feed.funding_rates)
            self.open(tm=feed.timestamp, prices=feed.open_prices, funding_rates=feed.funding_rates)
            
            # begin debug
            for exchange in self._exchanges.values():
                logging.debug(f"\n\n---------- before settle Exchange[{exchange.name}]")
                exchange.inspect()
            # end debug

            for market, trade in self._active_arb_trades.items():
                trade.settle(
                    ex2prices=feed.close_prices[market],
                    ex2markprices=feed.mark_prices[market],
                    ex2fundrates=feed.funding_rates[market],
                )
            if feed.timestamp.hour == 23:  # 每天结束时记录一次metrics
                trade.record_metrics(feed.timestamp)

            # begin debug
            for exchange in self._exchanges.values():
                logging.debug(f"\n\n---------- after settle Exchange[{exchange.name}]")
                exchange.inspect()
            # end debug

        # 退出循环时，feed指向最后一个feed
        for market, trade in self._active_arb_trades.items():
            self.__close(trade, feed.timestamp, feed.close_prices[market])
        trade.record_metrics(feed.timestamp)
