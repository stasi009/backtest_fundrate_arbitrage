from simulator.exchange import Exchange
from datetime import datetime


def test1():
    symbols = {"A": 0.1, "B": 0.1}
    cex = Exchange(name="test", init_cash=1000, markets=symbols, commission=0.01)

    cex.buy(market="A", price=100, shares=0.5)
    cex.sell(symbol="B", price=60, shares=0.3)
    cex.inspect()

    cex.settle_trading(prices={"A": 120, "B": 50})
    cex.inspect('settle')
    cex.record_metrics(datetime.now())
    
    cex.clear("B", price=59)
    cex.inspect()

    cex.settle_trading(prices={"A": 90, "B": 80})
    cex.inspect('settle')
    cex.record_metrics(datetime.now())

    print(cex.metric_history)


if __name__ == "__main__":
    test1()
