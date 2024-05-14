from simulator.cex_accounts import CexAccounts


class Order:
    def __init__(self, contract: str, cex: CexAccounts, is_long: int) -> None:
        self._contract = contract
        self._cex = cex
        self._is_long = is_long

        self.shares = None
        self._open_price = None
        self._close_price = None
        self._funding_pnl = 0

    @property
    def cex_name(self):
        return self._cex.name

    def open(self, usd_amount: float, price: float):
        self._shares = usd_amount / price
        self._open_price = price
        self._cex.trade(symbol=self._contract, is_long=self._is_long, price=price, shares=self._shares)

    def close(self, price: float):
        # is_long=-self._is_long，平仓时的交易方向与持仓方向相反
        self._cex.trade(symbol=self._contract, is_long=-self._is_long, price=price, shares=self._shares)
        self._close_price = price

    def accumulate_funding(self, mark_price, funding_rate):
        # _is_long>0==>long position, funding_rate>0==>long pay short, pnl<0
        # _is_long>0==>long position, funding_rate<0==>short pay long, pnl>0
        # _is_long<0==>short position, funding_rate<0==>short pay long, pnl<0
        # _is_long<0==>short position, funding_rate>0==>long pay short, pnl>0
        self._funding_pnl += -self._is_long * self._shares * mark_price * funding_rate

    @property
    def trade_pnl(self):
        # is_long>0，持有多仓，close price > open_price才profit
        # is_long<0，持有空仓，close price < open_price才profit
        pnl = self._is_long * (self._close_price - self._open_price) * self.shares

        for price in [self._open_price, self._close_price]:
            pnl -= price * self.shares * self._cex.commission

        return pnl
    
    @property
    def fund_pnl(self):
        return self._funding_pnl


class FundingArbitrageTrade:
    def __init__(self, contract: str, long_cex: CexAccounts, short_cex: CexAccounts) -> None:
        self._contract = contract  # 为了对冲，symbol肯定是唯一的

        self._orders = {
            "long": Order(contract=contract, cex=long_cex, is_long=1),
            "short": Order(contract=contract, cex=short_cex, is_long=-1),
        }

        self.is_active = False

    def open(self, usd_amount: float, prices: dict[str, float]):
        assert not self.is_active
        for k in ["long", "short"]:
            self._orders[k].open(usd_amount=usd_amount, price=prices[k])
        self.is_active = True

    def close(self, prices: dict[str, float]):
        assert self.is_active
        for k in ["long", "short"]:
            self._orders[k].close(price=prices[k])
        self.is_active = False

    def accumulate_funding(self, mark_prices, funding_rates):
        if not self.is_active:
            return
        for k in ["long", "short"]:
            self._orders[k].accumulate_funding(mark_price=mark_prices[k], funding_rate=funding_rates[k])
            
    @property
    def trade_pnl(self):
        assert not self.is_active # close_price is only available after closing the trade
        return sum(self._orders[k].trade_pnl for k in ["long", "short"])
    
    @property
    def fund_pnl(self):
        return sum(self._orders[k].fund_pnl for k in ["long", "short"])
        
        
