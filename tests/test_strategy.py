from simulator.config import Config
from simulator.strategy import FundingArbStrategy

HOURS_PER_YEAR = 24 * 365


def get_config():
    annual_fundrate_diff_open = 0.1
    annual_fundrate_diff_close = 0.01
    coins = ["btc", "eth", "sol"]

    return Config(
        init_cash=100000,
        margin_rate=0.5,
        commission=1 / 1000,
        slippage=0,
        ordersize_usd=1000,
        fundrate_diff_open=annual_fundrate_diff_open / HOURS_PER_YEAR,
        fundrate_diff_close=annual_fundrate_diff_close / HOURS_PER_YEAR,
        fundrate_diff_change_pct=0.1,
        data_dir="data/input",
        exchanges=["dydx", "rabbitx"],
        markets=[c.upper() + "-USD" for c in coins],
    )

def main():
    strategy = FundingArbStrategy(get_config())
    strategy.run()
    
    for exname, exchange in strategy.iter_exchanges():
        print(f'\n-------- {exname}')
        exchange.inspect()
    
if __name__ == "__main__":
    main()