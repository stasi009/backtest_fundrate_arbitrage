from simulator.exchange import Exchange
from datetime import datetime


class Order:
    def __init__(self, market: str, exchange: Exchange, is_long: int) -> None:
        self._market = market
        self._exchange = exchange
        self._is_long = is_long

        self.shares = None
        self._open_price = None
        self._close_price = None
        self._funding_pnl = 0

    @property
    def ex_name(self):
        return self._exchange.name

    def open(self, shares: float, price: float):
        self._shares = shares
        self._open_price = price
        self._exchange.trade(market=self._market, is_long=self._is_long, price=price, shares=self._shares)

    def close(self, price: float):
        # is_long=-self._is_long，平仓时的交易方向与持仓方向相反
        # TODO: 大部分情况下，这里也可以用exchange.close
        # 之所以没有使用，是因为还想保留一种可能性，就是针对同一个market，long in exchange A, short in exchange B & C
        self._exchange.trade(market=self._market, is_long=-self._is_long, price=price, shares=self._shares)
        self._close_price = price

    def accumulate_funding(self, mark_price, funding_rate):
        # _is_long>0==>long position, funding_rate>0==>long pay short, pnl<0
        # _is_long>0==>long position, funding_rate<0==>short pay long, pnl>0
        # _is_long<0==>short position, funding_rate<0==>short pay long, pnl<0
        # _is_long<0==>short position, funding_rate>0==>long pay short, pnl>0
        self._funding_pnl += -self._is_long * self._shares * mark_price * funding_rate

    @property
    def trade_pnl(self):
        # NOTE: 与mark to market的结果应该一致，
        # 因为close price - open price = close price - MarkToMarket Price + MarkToMarket Price - open price
        # is_long>0，持有多仓，close price > open_price才profit
        # is_long<0，持有空仓，close price < open_price才profit
        pnl = self._is_long * (self._close_price - self._open_price) * self.shares

        for price in [self._open_price, self._close_price]:
            pnl -= price * self.shares * self._exchange.commission

        return pnl

    @property
    def fund_pnl(self):
        return self._funding_pnl


class FundingArbTrade:
    def __init__(self, market: str, long_ex: Exchange, short_ex: Exchange) -> None:
        self.market = market  # 为了对冲，symbol肯定是唯一的

        self._orders = {
            "long": Order(market=market, exchange=long_ex, is_long=1),
            "short": Order(market=market, exchange=short_ex, is_long=-1),
        }

        self.open_tm: datetime = None  # 初次开仓的时间
        self.close_tm: datetime = None

        self._latest_fundrate_diff = None

    @property
    def is_active(self):
        return self.open_tm is not None and self.close_tm is None

    def match(self, market: str, long_ex: str, short_ex: str):
        return (
            self.market == market
            and self._orders["long"].ex_name == long_ex
            and self._orders["short"].ex_name == short_ex
        )

    def open(self, tm: datetime, usd_amount: float, prices: dict[str, float]):
        """
        Args:
            usd_amount: 因为不同market价格差异较大，很难统一设置交易份额，而设置交易金额比较直觉
            prices (dict[str, float]): exchange->price
        """
        shares = None
        for k in ["long", "short"]:
            tmp = usd_amount / prices[self._orders[k].ex_name]
            if shares is None or tmp < shares:
                shares = tmp

        for k in ["long", "short"]:
            order = self._orders[k]
            order.open(shares=shares, price=prices[order.ex_name])

        if self.open_tm is None:  # 加仓时不更新开仓时间
            self.open_tm = tm

    def close(self, tm: datetime, prices: dict[str, float]):
        """
        Args:
            prices (dict[str, float]): exchange->price
        """
        for k in ["long", "short"]:
            order = self._orders[k]
            order.close(price=prices[order.ex_name])
        self.close_tm = tm

    def accumulate_funding(self, mark_prices: dict[str, float], funding_rates: dict[str, float]):
        """
        Args:
            mark_prices (dict[str, float]): exchange->market price
            funding_rates (dict[str, float]): exchange->funding rate
        """
        if not self.is_active:
            return
        
        current_fundrates = {}

        for k in ["long", "short"]:
            order = self._orders[k]
            fundrate = funding_rates[order.ex_name]
            current_fundrates[k] = fundrate
            order.accumulate_funding(
                mark_price=mark_prices[order.ex_name], funding_rate=fundrate
            )

        self._latest_fundrate_diff = current_fundrates['short'] - current_fundrates['long']
        assert self._latest_fundrate_diff > 0

    @property
    def trade_pnl(self):
        assert not self.is_active  # close_price is only available after closing the trade
        return sum(self._orders[k].trade_pnl for k in ["long", "short"])

    @property
    def fund_pnl(self):
        return sum(self._orders[k].fund_pnl for k in ["long", "short"])
