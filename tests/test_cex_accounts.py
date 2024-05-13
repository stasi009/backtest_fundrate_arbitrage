from simulator.cex_accounts import CexAccounts
from datetime import datetime


def test1():
    symbols = {"A": 0.1, "B": 0.1}
    cex = CexAccounts(name="test", init_cash=1000, symbol_infos=symbols, commission=0.01)

    cex.buy(symbol="A", price=100, shares=0.5)
    cex.sell(symbol="B", price=60, shares=0.3)
    cex.inspect()

    cex.settle(datetime.now(), prices={"A": 120, "B": 50})
    cex.inspect('settle')
    
    cex.clear("B", price=59)
    cex.inspect()

    cex.settle(datetime.now(), prices={"A": 90, "B": 80})
    cex.inspect('settle')

    print(cex.metric_history)


if __name__ == "__main__":
    test1()
