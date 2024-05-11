
from simulator.cex_accounts import CexAccounts

def test1():
    symbols = {
        'A':0.1,
        'B':0.1
    }
    cex = CexAccounts(name='test',init_cash=1000,symbol_infos=symbols,commission=0.001)
    
    cex.buy(symbol='A',price=100,shares=0.5)
    
    cex.inspect()
    
if __name__ == "__main__":
    test1()
    