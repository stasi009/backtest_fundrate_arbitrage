import numpy as np
import pandas as pd
import logging
from dataclasses import dataclass,replace
from prettytable import PrettyTable


@dataclass
class PerpsAccount:
    symbol: str
    margin_rate: float
    long_short_shares = 0  # >0, long; <0, short.之所以不叫shares，提醒我这个shares能正能负
    hold_price = 0
    used_margin = 0
    trade_pnl = 0  # 由买卖产生的PnL
    fund_pnl = 0  # 根据funding rate带来的支出或收入


class NotEnoughMargin(Exception):
    def __init__(self, margin_call: bool) -> None:
        self.margin_call = margin_call


class Accounts:
    def __init__(
        self, name: str, init_cash: float, symbol_infos: dict[str, float], commission=0.00005
    ) -> None:
        self.name = name

        self.__init_cash = init_cash
        self._cash = init_cash  # 可用资金
        self.commission = commission

        self._perps_accounts = {
            symbol: PerpsAccount(symbol, margin_rate) for symbol, margin_rate in symbol_infos.items()
        }

        self._metrics = []

    def __update_cash(self, delta_cash: float, need_margincall: bool):
        """
        need_margincall的设置标准
        - 如果是主动交易行为，设置need_margincall=False，因为我们干脆放弃这次交易就好了，就不会触发margin call了
        - 如果是mark to market导致的保证金不足，就一定要触发margin call，终止回测
        """
        temp = self._cash + delta_cash
        if temp <= 0:
            logging.critical(f"Not Enough Margin: original cash={self._cash},delta_cash={delta_cash}")
            raise NotEnoughMargin(need_margincall)
        self._cash = temp

    def _close(self, symbol: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[symbol]

        # is_long>0，买入平仓，说明平的是空仓，price < hold_price才profit
        # is_long<0，卖出平仓，说明平的是多仓，price > hold_price才profit
        pnl = -is_long * (price - account.hold_price) * shares
        self.__update_cash(pnl, need_margincall=False)
        account.trade_pnl += pnl

        reduce_margin = shares / abs(account.long_short_shares) * account.used_margin  # 肯定是个正数
        account.used_margin -= reduce_margin  # 释放保证金
        self.__update_cash(reduce_margin, need_margincall=False)

        # is_long>0，买入平仓，说明平的是空仓，原来的long_short_shares<0，加上正shares，持仓才变小
        # is_long<0，卖出平仓，说明平的是多仓，原来的long_short_shares>0，加上负shares，持仓才变小
        # 另外，平仓时不用更新hold price，因为PnL被转移到cash账户中了，不在资产帐户中
        account.long_short_shares += is_long * shares
        
        logging.info(f'[{self.name}] --CLOSE-- {'BUY' if is_long else 'SELL'} [{symbol}] at price={price:.2f} for {shares} shares')

    def _open(self, symbol: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[symbol]

        new_margin = shares * price * account.margin_rate  # 新建仓位需要的保证金
        self.__update_cash(-new_margin, need_margincall=False)
        account.used_margin += new_margin

        total_cost = abs(account.long_short_shares) * account.hold_price + shares * price
        account.long_short_shares += is_long * shares
        new_hold_price = total_cost / abs(account.long_short_shares)
        assert 0 < new_hold_price < account.hold_price
        
        account.hold_price = new_hold_price
        logging.info(f'[{self.name}] ++OPEN++ {'BUY' if is_long else 'SELL'} [{symbol}] at price={price:.2f} for {shares} shares')

    def trade(self, symbol: str, is_long: int, price: float, shares: float) -> None:
        account = self._perps_accounts[symbol]

        if is_long * account.long_short_shares >= 0:  # 本次交易方向与目前持仓方向相同，无需先平仓
            close_shares = 0
        else:
            close_shares = min(abs(account.long_short_shares), shares)
        open_shares = shares - close_shares

        fee = price * shares * self.commission
        self.__update_cash(-fee, need_margincall=False)
        account.trade_pnl -= fee

        if close_shares > 0:  # 先平仓
            self._close(symbol=symbol, is_long=is_long, price=price, shares=shares)

        if open_shares > 0:
            self._open(symbol=symbol, is_long=is_long, price=price, shares=shares)

    def buy(self, symbol: str, price: float, shares: float):
        self.trade(symbol=symbol, is_long=1, price=price, shares=shares)

    def sell(self, symbol: str, price: float, shares: float):
        self.trade(symbol=symbol, is_long=-1, price=price, shares=shares)

    def clear(self, symbol: str, price: float):
        account = self._perps_accounts[symbol]
        is_long = 1 if account.long_short_shares < 0 else -1  # 平仓时的交易方向肯定与当前持仓方向相反
        self.trade(symbol=symbol, is_long=is_long, price=price, shares=abs(account.long_short_shares))

    @property
    def _current_metric(self):
        total_used_margin = 0
        total_trade_pnl = 0
        total_fund_pnl = 0
        for _, account in self._perps_accounts.items():
            total_used_margin += account.used_margin
            total_trade_pnl += account.trade_pnl
            total_fund_pnl += account.fund_pnl
        total_value = self._cash + total_used_margin
        assert abs(self.__init_cash + total_trade_pnl + total_fund_pnl - total_value) < 1e-6

        return dict(
            used_margin=total_used_margin,
            cash=self._cash,
            total_value=self._cash + total_used_margin,
            trade_pnl=total_trade_pnl,
            fund_pnl=total_fund_pnl,
        )

    def trading_settle(self, prices):
        """
        timestamp和prices都由pd.DataFrame.iterrows获得
        - timestamp是某一行的index，代表时间
        - prices是一个pd.Series, prices[symbol]表示该symbol的价格
        - !TODO:显然这里做了极大的简化，认为一个时间段内只有一个价格
        """
        for symbol, account in self._perps_accounts.items():
            price = prices[symbol]
            if np.isnan(price):
                continue

            # ----------- mark to market
            # long_short_shares>0，持有多仓，price>hold_price才profit
            # long_short_shares<0，持有空仓，price<hold_price才profit
            pnl = (price - account.hold_price) * account.long_short_shares
            self.__update_cash(pnl, need_margincall=True)
            account.trade_pnl += pnl
            account.hold_price = price  # mark to market

            # ----------- new margin requirement
            new_margin = abs(account.long_short_shares) * price * account.margin_rate
            margin_diff = new_margin - account.used_margin
            self.__update_cash(-margin_diff, need_margincall=True)
            account.used_margin += margin_diff
            
    def funding_settle(self,mark_prices:dict[str,float],funding_rates:dict[str,float]):
        for symbol, account in self._perps_accounts.items():
            mark_price = mark_prices[symbol]
            funding_rate = funding_rates[symbol]
            if np.isnan(mark_price) or np.isnan(funding_rate):
                continue
            
            # long_short_shares>0==>long position, funding_rate>0==>long pay short, pnl<0
            # long_short_shares>0==>long position, funding_rate<0==>short pay long, pnl>0
            # long_short_shares<0==>short position, funding_rate<0==>short pay long, pnl<0
            # long_short_shares<0==>short position, funding_rate>0==>long pay short, pnl>0
            pnl = -funding_rate * account.long_short_shares * mark_price
            self.__update_cash(pnl,need_margincall=True)
            account.fund_pnl += pnl
            

    def record_metric(self, timestamp) -> None:
        # ------------ calculate metrics
        metric = self._current_metric
        metric["timestamp"] = timestamp
        self._metrics.append(metric)

    @property
    def metric_history(self):
        df = pd.DataFrame(self._metrics)
        df.set_index("timestamp", inplace=True)
        return df

    def inspect(self, header: str = ""):
        print(f"\n\n************************* {header}")
        # ---------- summary
        metric_keys = ["total_value", "cash", "used_margin", "trade_pnl", "fund_pnl"]
        pt = PrettyTable(metric_keys, title="Summary")
        metric = self._current_metric
        pt.add_row([f"{metric[k]:.3f}" for k in metric_keys])
        print(pt)
        # ---------- each account
        pt = PrettyTable(
            ["Symbol", "Shares", "HoldPrice", "UsedMargin", "TradePnL", "FundPnL"], title="Perps Accounts"
        )
        for symbol, account in self._perps_accounts.items():
            pt.add_row(
                (
                    symbol,
                    f"{account.long_short_shares:.3f}",
                    f"{account.hold_price:.3f}",
                    f"{account.used_margin:.3f}",
                    f"{account.trade_pnl:.3f}",
                    f"{account.fund_pnl:.3f}",
                )
            )
        print(pt)
