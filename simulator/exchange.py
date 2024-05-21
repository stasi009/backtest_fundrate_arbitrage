import pandas as pd
import logging
from prettytable import PrettyTable
from enum import Enum

# 资金变化反映在哪个会议科目上
CashItem = Enum("CashItem", ["MARGIN", "TRADE_PNL", "FUND_PNL"])


class PerpsAccount:
    def __init__(self, market: str, margin_rate: float, cash_callback) -> None:
        self.market = market
        self.margin_rate = margin_rate

        self.long_short_shares = 0  # >0, long; <0, short.之所以不叫shares，提醒我这个shares能正能负
        self.hold_price = 0
        self.used_margin = 0
        self.trade_pnl = 0  # 由买卖产生的PnL
        self.fund_pnl = 0  # 根据funding rate带来的支出或收入

        self._cash_callback = cash_callback

    def update(self, cash_item: CashItem, delta_cash: float):
        """account账户下的cash_item这个会计科目，导致了delta_cash的资金变化
        - cash_item：资金变化反映在哪个会计科目上
        - delta_cash：delta_cash>0(<0)说明该变化导致cash增加（减少）
        """
        self._cash_callback(delta_cash)

        match cash_item:
            case CashItem.MARGIN:
                # delta_cash>0，cash账户增加，是因为释放保证金，所以used_margin减少
                # delta_cash<0，cash账户减少，是因为追加保证金，所以used_margin增加
                self.used_margin -= delta_cash
            case CashItem.TRADE_PNL:
                self.trade_pnl += delta_cash  # PnL的变化应该与cash变化同向
            case CashItem.FUND_PNL:
                self.fund_pnl += delta_cash
            case _:
                raise ValueError(f"Unknown CashItem={cash_item}")


class MarginCall(Exception):
    pass


class Exchange:
    def __init__(self, name: str, init_cash: float, markets: dict[str, float], commission=0.00005) -> None:
        self.name = name

        self.__init_cash = init_cash
        self._cash = init_cash  # 可用资金
        self.commission = commission

        self._perps_accounts = {
            market: PerpsAccount(market, margin_rate, cash_callback=self._update_cash)
            for market, margin_rate in markets.items()
        }

        self._metrics = []

    def get_account(self, market: str) -> PerpsAccount:
        return self._perps_accounts[market]
    
    def set_account(self,market:str, account:PerpsAccount)->None:
        """ 主要用于回滚操作，将某个market状态回滚至操作前的状态
        """
        self._perps_accounts[market] = account
        

    def _update_cash(self, delta_cash: float):
        temp = self._cash + delta_cash
        if temp <= 0:
            logging.critical(f"Not Enough Margin: original cash={self._cash},delta_cash={delta_cash}")
            raise MarginCall()
        self._cash = temp

    def _close(self, market: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[market]

        reduce_margin = shares / abs(account.long_short_shares) * account.used_margin  # 肯定是个正数
        account.update(cash_item=CashItem.MARGIN, delta_cash=reduce_margin)

        # is_long>0，买入平仓，说明平的是空仓，price < hold_price才profit
        # is_long<0，卖出平仓，说明平的是多仓，price > hold_price才profit
        pnl = -is_long * (price - account.hold_price) * shares
        account.update(cash_item=CashItem.TRADE_PNL, delta_cash=pnl)

        # is_long>0，买入平仓，说明平的是空仓，原来的long_short_shares<0，加上正shares，持仓才变小
        # is_long<0，卖出平仓，说明平的是多仓，原来的long_short_shares>0，加上负shares，持仓才变小
        # 另外，平仓时不用更新hold price，因为PnL被转移到cash账户中了，不在资产帐户中
        account.long_short_shares += is_long * shares

        logging.info(
            f"[{self.name}] --CLOSE-- {'BUY' if is_long else 'SELL'} [{market}] at price={price:.2f} for {shares} shares"
        )

    def _open(self, market: str, is_long: int, price: float, shares: float):
        account = self._perps_accounts[market]

        new_margin = shares * price * account.margin_rate  # 新建仓位需要的保证金
        account.update(cash_item=CashItem.MARGIN, delta_cash=-new_margin)

        total_cost = abs(account.long_short_shares) * account.hold_price + shares * price
        account.long_short_shares += is_long * shares
        new_hold_price = total_cost / abs(account.long_short_shares)
        assert (account.hold_price == 0) or (0 < new_hold_price < account.hold_price)

        account.hold_price = new_hold_price
        logging.info(
            f"[{self.name}] ++OPEN++ {'BUY' if is_long else 'SELL'} [{market}] at price={price:.2f} for {shares} shares"
        )

    def trade(self, market: str, is_long: int, price: float, shares: float) -> None:
        assert shares > 0
        account = self._perps_accounts[market]

        if is_long * account.long_short_shares >= 0:  # 本次交易方向与目前持仓方向相同，无需先平仓
            close_shares = 0
        else:
            close_shares = min(abs(account.long_short_shares), shares)
        open_shares = shares - close_shares

        fee = price * shares * self.commission
        account.update(cash_item=CashItem.TRADE_PNL, delta_cash=-fee)

        if close_shares > 0:  # 先平仓
            self._close(market=market, is_long=is_long, price=price, shares=shares)

        if open_shares > 0:
            self._open(market=market, is_long=is_long, price=price, shares=shares)

    def buy(self, market: str, price: float, shares: float):
        self.trade(market=market, is_long=1, price=price, shares=shares)

    def sell(self, market: str, price: float, shares: float):
        self.trade(market=market, is_long=-1, price=price, shares=shares)

    def clear(self, market: str, price: float):
        account = self._perps_accounts[market]
        is_long = 1 if account.long_short_shares < 0 else -1  # 平仓时的交易方向肯定与当前持仓方向相反
        self.trade(market=market, is_long=is_long, price=price, shares=abs(account.long_short_shares))

    @property
    def _current_metric(self):
        total_used_margin = 0
        total_trade_pnl = 0
        total_fund_pnl = 0
        for account in self._perps_accounts.values():
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

    def trading_settle(self, market: str, price: float):
        account = self._perps_accounts[market]

        # ----------- mark to market
        # long_short_shares>0，持有多仓，price>hold_price才profit
        # long_short_shares<0，持有空仓，price<hold_price才profit
        pnl = (price - account.hold_price) * account.long_short_shares
        account.update(cash_item=CashItem.TRADE_PNL, delta_cash=pnl)
        account.hold_price = price  # mark to market

        # ----------- new margin requirement
        new_margin = abs(account.long_short_shares) * price * account.margin_rate
        margin_diff = new_margin - account.used_margin
        account.update(cash_item=CashItem.MARGIN, delta_cash=-margin_diff)

    def funding_settle(self, market: str, mark_price: float, funding_rate: float):
        account = self._perps_accounts[market]

        # long_short_shares>0==>long position, funding_rate>0==>long pay short, pnl<0
        # long_short_shares>0==>long position, funding_rate<0==>short pay long, pnl>0
        # long_short_shares<0==>short position, funding_rate<0==>short pay long, pnl<0
        # long_short_shares<0==>short position, funding_rate>0==>long pay short, pnl>0
        pnl = -funding_rate * account.long_short_shares * mark_price
        account.update(cash_item=CashItem.FUND_PNL, delta_cash=pnl)

    def record_metric(self, timestamp) -> None:
        # ------------ calculate metrics
        metric = self._current_metric
        metric["timestamp"] = timestamp
        self._metrics.append(metric)

    @property
    def metric_history(self):
        df = pd.DataFrame(self._metrics)
        df.set_index("timestamp", inplace=True)
        df = df.loc[:, ["total_value", "cash", "used_margin", "trade_pnl", "fund_pnl"]]  # reorder columns
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
            ["Market", "Shares", "HoldPrice", "UsedMargin", "TradePnL", "FundPnL"], title="Perps Accounts"
        )
        for market, account in self._perps_accounts.items():
            pt.add_row(
                (
                    market,
                    f"{account.long_short_shares:.3f}",
                    f"{account.hold_price:.3f}",
                    f"{account.used_margin:.3f}",
                    f"{account.trade_pnl:.3f}",
                    f"{account.fund_pnl:.3f}",
                )
            )
        print(pt)
