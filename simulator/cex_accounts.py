class PerpsAccount:
    def __init__(self, symbol:str, margin_rate:float) -> None:
        self.symbol = symbol

        self.long_short_shares = 0  # >0, long; <0, short
        self.hold_price = 0

        self.margin_rate = margin_rate
        self.used_margin = 0

        self.fee = 0
        self.unrealized_pnl = 0
        self.realized_pnl = 0
        self.funding_amount = 0  # 根据funding rate带来的支出或收入

class MarginCall(Exception):
    pass


class CexAccounts:
    def __init__(self, init_cash: float, symbol_infos: dict[str, float], commission=0.00005) -> None:
        self._cash = init_cash
        self._commission = commission

        self._perps_accounts = {
            symbol: PerpsAccount(symbol, margin_rate) for symbol, margin_rate in symbol_infos.items()
        }
        
    def _close(self,symbol: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[symbol]
        
        # is_long>0，买入平仓，说明平的是空仓，price < hold_price才profit
        # is_long<0，卖出平仓，说明平的是多仓，price > hold_price才profit
        pnl = -is_long * (price - account.hold_price) * shares 
        self._cash += pnl 
        account.realized_pnl += pnl
        
        # is_long>0，买入平仓，说明平的是空仓，原来的long_short_shares<0，加上正shares，持仓才变小
        # is_long<0，卖出平仓，说明平的是多仓，原来的long_short_shares>0，加上负shares，持仓才变小
        account.long_short_shares += is_long * shares
        
        reduce_margin = shares / abs(account.long_short_shares) * account.used_margin#肯定是个正数
        account.used_margin -= reduce_margin # 释放保证金
        self._cash += reduce_margin
        
    def _open(self,symbol: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[symbol]

        new_margin = shares * price * account.margin_rate 
        if new_margin >= self._cash:
            raise MarginCall()
        account.used_margin += new_margin
        self._cash -= new_margin
        
        total_cost = abs(account.long_short_shares) * account.hold_price + shares * price
        account.long_short_shares += is_long * shares
        account.hold_price = total_cost / abs(account.long_short_shares)

    def trade(self, symbol: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[symbol]

        if is_long * account.long_short_shares >= 0:  # 本次交易方向与目前持仓方向相同，无需先平仓
            close_shares = 0
        else:
            close_shares = min(abs(account.long_short_shares), shares)
        open_shares = shares - close_shares
        
        if close_shares >0:
            self._close(symbol=symbol,is_long=is_long,price=price,shares=shares)
            
        if open_shares >0:
            self._open(symbol=symbol,is_long=is_long,price=price,shares=shares)
            
    def mark_to_market(self,timestamp,prices):
        """ 
        timestamp和prices都由pd.DataFrame.iterrows获得
        - timestamp是某一行的index，代表时间
        - prices是一个pd.Series, prices[symbol]表示该symbol的价格
        """
        for symbol, account in self._perps_accounts.items():
            price = prices[symbol]