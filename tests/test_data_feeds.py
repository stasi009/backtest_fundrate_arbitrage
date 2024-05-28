
from simulator.data_feeds import DataFeeds
from pprint import pprint

def test():
    coins = ['btc','eth','sol']
    data_feeds = DataFeeds(
            data_dir='data/input', 
            exchanges=['dydx','rabbitx'], 
            markets=[c.upper()+'-USD' for c in coins]
        )
    
    for idx, feed in enumerate(data_feeds,start=1):
        print(idx)
        
if __name__ == "__main__":
    test()