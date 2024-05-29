from simulator.exchange import Exchange, MarginCall, PerpsAccount
from simulator.utils import Config, hfr2a
from dataclasses import dataclass
from datetime import datetime
from copy import copy
import logging


@dataclass
class BackupOrder:
    account: PerpsAccount
    has_init_account: bool
    cash: float


class Order:
    def __init__(self, market: str, exchange: Exchange, is_long: int, slippage: float) -> None:
        self._market = market
        self._exchange = exchange
        self._is_long = is_long
        self._init_account = None
        self._slippage = slippage

    @property
    def ex_name(self):
        return self._exchange.name

    @property
    def backup(self) -> BackupOrder:
        return BackupOrder(
            account=copy(self._exchange.get_account(self._market)),
            has_init_account=self._init_account is not None,
            cash=self._exchange.cash,
        )

    def restore(self, backup: BackupOrder) -> None:
        self._exchange.set_account(self._market, backup.account)
        self._exchange.cash = backup.cash
        if not backup.has_init_account:
            self._init_account = None

    def slip_price(self, price: float):
        # is_long >0，滑点使买得更昂贵
        # is_long <0，滑点使卖得更便宜
        return price * (1 + self._is_long * self._slippage)

    def open(self, shares: float, price: float):
        if self._init_account is None:
            # open可用于加仓，所以只在第1次open时才快照
            self._init_account = copy(self._exchange.get_account(self._market))
        self._exchange.trade(
            market=self._market,
            is_long=self._is_long,
            price=self.slip_price(price),
            shares=shares,
        )

    def close(self, price: float):
        # 套利只存在一个long ex和一个short ex之间，不存在multi long ex vs. multi short ex的情况
        # 换仓也会先把原来的仓位关掉，所以可以放心clear所有仓位
        self._exchange.clear(market=self._market, price=self.slip_price(price))

    def settle(self, contract_price: float, mark_price: float, funding_rate: float):
        trade_pnl, margin_diff = self._exchange.settle_trading(market=self._market, price=contract_price)
        fund_pnl = self._exchange.settle_funding(
            market=self._market, mark_price=mark_price, funding_rate=funding_rate
        )

        logging.info(
            f"[{self._exchange.name:>8}] {'Long' if self._is_long > 0 else 'SELL'} {self._market}, "
            f"TradePnl={trade_pnl:.4f}, MarginDiff={margin_diff:.4f}, FundPnl={fund_pnl:.4f}"
        )

    def record_metrics(self, timestamp: datetime):
        self._exchange.record_metrics(timestamp)

    @property
    def trade_pnl(self):
        current_account = self._exchange.get_account(self._market)
        return current_account.trade_pnl - self._init_account.trade_pnl

    @property
    def fund_pnl(self):
        current_account = self._exchange.get_account(self._market)
        return current_account.fund_pnl - self._init_account.fund_pnl


class FundingArbTrade:

    def __init__(self, market: str, long_ex: Exchange, short_ex: Exchange, config: Config) -> None:
        self.market = market
        self._config = config

        self._orders = {
            "long": Order(market=market, exchange=long_ex, is_long=1, slippage=config.slippage),
            "short": Order(market=market, exchange=short_ex, is_long=-1, slippage=config.slippage),
        }

        self.open_tm: datetime = None  # 初次开仓的时间
        self.close_tm: datetime = None

        self.latest_fundrate_diff: float = None  # 最近一次的funding rate diff
        self.open_fundrate_diff: float = None  # 开仓时的funding rate diff

        self.trade_pnl: float = None
        self.fund_pnl: float = None

    @property
    def is_active(self):
        return self.open_tm is not None and self.close_tm is None

    @property
    def name(self):
        return f"L[{self._orders['long'].ex_name}].S[{self._orders['short'].ex_name}].{self.market}"

    def safe_open(self, tm: datetime, usd_amount: float, ex2prices: dict[str, float], fundrate_diff: float):
        """如果本次开仓导致margin call，回滚对账户的修改，相当于放弃本次操作
        无论long ex or short ex哪个发生margin call，两个ex都要回滚，因为只单边建仓是没有对冲的，极其危险的
        """
        backups = {direction: order.backup for direction, order in self._orders.items()}
        try:
            self._open(tm=tm, usd_amount=usd_amount, ex2prices=ex2prices, fundrate_diff=fundrate_diff)
            return True
        except MarginCall:
            logging.error(f"!!! Margin Call on {self.name}, Drop Open Actions")
            for direction, order in self._orders.items():
                order.restore(backups[direction])
            return False

    def _open(self, tm: datetime, usd_amount: float, ex2prices: dict[str, float], fundrate_diff: float):
        """
        Args:
            usd_amount: 因为不同market价格差异较大，很难统一设置交易份额，而设置交易金额比较直觉
            prices (dict[str, float]): exchange->price
        """
        shares = None  # 必须买卖相同shares才能对冲delta
        for order in self._orders.values():
            tmp = usd_amount / ex2prices[order.ex_name]
            if shares is None or tmp < shares:
                shares = tmp

        for order in self._orders.values():
            order.open(shares=shares, price=ex2prices[order.ex_name])

        # 只有两个order都open成功而不抛出异常，下列代码才会执行，as expected
        self.open_fundrate_diff = fundrate_diff
        assert self.open_fundrate_diff > 0
        self.latest_fundrate_diff = self.open_fundrate_diff

        if self.open_tm is None:  # 加仓时不更新开仓时间
            self.open_tm = tm

    def close(self, tm: datetime, ex2prices: dict[str, float]):
        """
        Args:
            prices (dict[str, float]): exchange->price
        """
        self.trade_pnl = 0
        self.fund_pnl = 0

        for order in self._orders.values():
            order.close(price=ex2prices[order.ex_name])
            self.trade_pnl += order.trade_pnl  # 固化下来
            self.fund_pnl += order.fund_pnl

        self.close_tm = tm

    def diff_fundrates(self, ex2fundrates: dict[str, float]):
        current_fundrates = {
            direction: ex2fundrates[order.ex_name] for direction, order in self._orders.items()
        }
        # 如果两个fundrate都正，在fundrate更小的ex long，支付较少funding，在fundrate更大的ex short，收取较多的funding
        # 如果两个fundrate都负，在fundrate更负的ex long，收取较多funding，在abs(fundrate)小的ex short，支付较少funding
        # 如果两个fundrate一正一负，在fundrate<0的ex long，收取funding，在fundrate>0的ex short，收取funding
        short_fr = current_fundrates["short"]
        long_fr = current_fundrates["long"]
        self.latest_fundrate_diff = short_fr - long_fr
        logging.info(
            f"Trade[{self.name}] ASFR={hfr2a(short_fr):.2%}"
            f", ALFR={hfr2a(long_fr):.2%}"
            f", AFRdiff={hfr2a(self.latest_fundrate_diff):.2%}"
        )
        return self.latest_fundrate_diff

    def settle(
        self, ex2prices: dict[str, float], ex2markprices: dict[str, float], ex2fundrates: dict[str, float]
    ):
        assert self.is_active

        for order in self._orders.values():
            order.settle(
                contract_price=ex2prices[order.ex_name],
                mark_price=ex2markprices[order.ex_name],
                funding_rate=ex2fundrates[order.ex_name],
            )

        # latest_fundrate_diff<=0的，在settle之前就已经关闭了
        assert self.latest_fundrate_diff > 0

    def record_metrics(self, timestamp: datetime):
        for order in self._orders.values():
            order.record_metrics(timestamp)
