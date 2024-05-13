from simulator.cex_accounts import CexAccounts


class Order:
    def __init__(self, contract: str, cex: str, commission: float, is_long: int) -> None:
        self._contract = contract
        self._cex = cex
        self._commission = commission
        self._is_long = is_long

        self.shares = None
        self._open_price = None
        self._close_price = None

    def open(self, usd_amount: float, open_price: float):
        self.shares = usd_amount / open_price
        self._open_price = open_price

    def close(self, close_price: float):
        self._close_price = close_price

    @property
    def trade_pnl(self):
        # is_long>0，持有多仓，close price > open_price才profit
        # is_long<0，持有空仓，close price < open_price才profit
        pnl = self._is_long * (self._close_price - self._open_price) * self.shares

        for price in [self._open_price, self._close_price]:
            pnl -= price * self.shares * self._commission

        return pnl


class FundingArbitrageTrade:
    def __init__(self, contract: str, long_cex: CexAccounts, short_cex: CexAccounts) -> None:
        self._contract = contract  # 为了对冲，symbol肯定是唯一的

        self._long_cex = long_cex
        self._long_order = Order(
            contract=contract, cex=long_cex.name, commission=long_cex.commission, is_long=1
        )

        self._short_cex = short_cex
        self._short_order = Order(
            contract=contract, cex=short_cex.name, commission=short_cex.commission, is_long=-1
        )

    def open(self, usd_amount: float, long_cex_price: float, short_cex_price: float):
        self._long_order.open(usd_amount=usd_amount, open_price=long_cex_price)
        self._long_cex.buy(symbol=self._contract, price=long_cex_price, shares=self._long_order.shares)

        self._short_order.open(usd_amount=usd_amount, open_price=short_cex_price)
        self._short_cex.sell(symbol=self._contract, price=short_cex_price, shares=self._short_order.shares)

    def close(self, long_cex_price: float, short_cex_price: float):
        self._long_order.close(long_cex_price)
        self._short_order.close(short_cex_price)
