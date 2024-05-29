from simulator.utils import Config, afr2h
from simulator.strategy import FundingArbStrategy
from prettytable import PrettyTable
import logging


def get_config():
    annual_fr_diff_open = 0.1
    annual_fr_diff_close = 0.01
    coins = ["btc", "eth", "sol"]

    return Config(
        init_cash=100000,
        margin_rate=0.5,
        commission=1 / 1000,
        slippage=0,
        ordersize_usd=1000,
        fundrate_diff_open=afr2h(annual_fr_diff_open),
        fundrate_diff_close=afr2h(annual_fr_diff_close),
        fundrate_diff_change_pct=0.1,
        data_dir="data/input",
        exchanges=["dydx", "rabbitx"],
        markets=[c.upper() + "-USD" for c in coins],
    )


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(message)s", filename="backtest.log", filemode="wt")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger("").addHandler(console)


def main():
    strategy = FundingArbStrategy(get_config())
    strategy.run()

    for exchange in strategy.iter_exchanges():
        print(f"\n\n************************* Post Backtest: Exchange[{exchange.name}]")
        exchange.inspect()
        print(exchange.metric_history)

    print(f"\n\n************************* ALL TRADES")
    total_trade_pnl = 0
    total_fund_pnl = 0
    pt = PrettyTable(["index", "open time", "close time", "trade PnL", "fund PnL"], float_format=".3")
    for idx, trade in enumerate(strategy.closed_trades, start=1):
        total_trade_pnl += trade.trade_pnl
        total_fund_pnl += trade.fund_pnl
        pt.add_row([idx, trade.open_tm, trade.close_tm, trade.trade_pnl, trade.fund_pnl])
    print(pt)

    pt = PrettyTable(["Item", "Value"], float_format=".3")
    pt.add_row(["Total Trade Pnl", total_trade_pnl])
    pt.add_row(["Total Funding Pnl", total_fund_pnl])
    pt.add_row(["Total PnL", total_trade_pnl + total_fund_pnl])
    print(pt)


if __name__ == "__main__":
    setup_logging()
    main()
