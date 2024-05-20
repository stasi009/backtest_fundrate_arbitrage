from simulator.exchange import Exchange
from simulator.config import Config
from datetime import datetime
from copy import copy

class Order:
    def __init__(self, market: str, exchange: Exchange, is_long: int) -> None:
        self._market = market
        self._exchange = exchange
        self._is_long = is_long
        self._init_account = None

    @property
    def ex_name(self):
        return self._exchange.name

    def open(self, shares: float, price: float):
        if self._init_account is None:
            # open可用于加仓，所以只在第1次open时才快照
            self._init_account = copy(self._exchange.account(self._market))
        self._exchange.trade(market=self._market, is_long=self._is_long, price=price, shares=self._shares)

    def close(self, price: float):
        # is_long=-self._is_long，平仓时的交易方向与持仓方向相反
        # TODO: 大部分情况下，这里也可以用exchange.close
        # 之所以没有使用，是因为还想保留一种可能性，就是针对同一个market，long in exchange A, short in exchange B & C
        self._exchange.trade(market=self._market, is_long=-self._is_long, price=price, shares=self._shares)

    def settle(self, contract_price: float, mark_price:float, funding_rate:float):
        self._exchange.trading_settle(market=self._market, price=contract_price)
        self._exchange.funding_settle(market=self._market, mark_price=mark_price, funding_rate=funding_rate)

    @property
    def trade_pnl(self):
        current_account = self._exchange.account(self._market)
        return current_account.trade_pnl - self._init_account.trade_pnl
        

    @property
    def fund_pnl(self):
        current_account = self._exchange.account(self._market)
        return current_account.fund_pnl - self._init_account.fund_pnl
    
    @property
    def used_margin(self):
        return self._exchange.account(self._market).used_margin


class FundingArbTrade:

    def __init__(self, market: str, long_ex: Exchange, short_ex: Exchange, config: Config) -> None:
        self.market = market  # 为了对冲，symbol肯定是唯一的
        self._config = config

        self._orders = {
            "long": Order(market=market, exchange=long_ex, is_long=1),
            "short": Order(market=market, exchange=short_ex, is_long=-1),
        }

        self.open_tm: datetime = None  # 初次开仓的时间
        self.close_tm: datetime = None

        self.latest_fundrate_diff = None
        self.open_fundrate_diff = None

    @property
    def is_active(self):
        return self.open_tm is not None and self.close_tm is None

    @property
    def name(self):
        return f"L[{self._orders['long'].ex_name}].S[{self._orders['short'].ex_name}].{self.market}"

    def open(self, tm: datetime, usd_amount: float, ex2prices: dict[str, float], fundrate_diff: float):
        """
        Args:
            usd_amount: 因为不同market价格差异较大，很难统一设置交易份额，而设置交易金额比较直觉
            prices (dict[str, float]): exchange->price
        """
        shares = None
        for k in ["long", "short"]:
            tmp = usd_amount / ex2prices[self._orders[k].ex_name]
            if shares is None or tmp < shares:
                shares = tmp

        for k in ["long", "short"]:
            order = self._orders[k]
            order.open(shares=shares, price=ex2prices[order.ex_name])

        self.open_fundrate_diff = fundrate_diff
        assert self.open_fundrate_diff > 0

        if self.open_tm is None:  # 加仓时不更新开仓时间
            self.open_tm = tm

    def close(self, tm: datetime, ex2prices: dict[str, float]):
        """
        Args:
            prices (dict[str, float]): exchange->price
        """
        for k in ["long", "short"]:
            order = self._orders[k]
            order.close(price=ex2prices[order.ex_name])
        self.close_tm = tm

    def accumulate_funding(self, ex2markprices: dict[str, float], ex2fundrates: dict[str, float]):
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
            fundrate = ex2fundrates[order.ex_name]
            current_fundrates[k] = fundrate
            order.accumulate_funding(mark_price=ex2markprices[order.ex_name], funding_rate=fundrate)

        self.latest_fundrate_diff = current_fundrates["short"] - current_fundrates["long"]
        assert self.latest_fundrate_diff > 0

    @property
    def trade_pnl(self):
        assert not self.is_active  # close_price is only available after closing the trade
        return sum(self._orders[k].trade_pnl for k in ["long", "short"])

    @property
    def fund_pnl(self):
        return sum(self._orders[k].fund_pnl for k in ["long", "short"])
