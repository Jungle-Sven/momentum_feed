from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES, BID, ASK, BLOCKCHAIN, FUNDING, GEMINI, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, PERPETUAL, TICKER, TRADES, INDEX
from cryptofeed.exchanges.dydx import dYdX

from cryptofeed.exchanges import Binance, BinanceFutures

from decimal import Decimal

import redis

import json

from datetime import datetime

# Connect to the Redis server
redis_host = 'momentum_redis_container'  # Change this to the Redis server address if running on a different machine
redis_port = 6379         # Default Redis port
redis_db = 0              # Redis database number
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

# Custom JSON encoder class to handle Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class Connector:
    def __init__(self):
        self.fh = FeedHandler()
        self.max_length = 100000
        self.markets = {
            'dYdX': ['BTC-USD-PERP', 'ETH-USD-PERP'],
            'binance': ['BTC-USDT', 'ETH-USDT'],
            'binance-futures': ['BTC-USDT-PERP', 'ETH-USDT-PERP']
        }
        
    def fix_market_names(self, market):
        if 'BTC' in market:
            return 'BTC-USD'
        elif 'ETH' in market:
            return 'ETH-USD'
        else:
            return market
    
    def run(self):
        #self.fh.add_feed(Binance(symbols=self.markets['binance'], channels=[TRADES], callbacks={TRADES: self.trade}))
        #self.fh.add_feed(Binance(symbols=self.markets['binance'], channels=[L2_BOOK], callbacks={L2_BOOK: self.book}))
        self.fh.add_feed(BinanceFutures(symbols=self.markets['binance-futures'], channels=[TRADES], callbacks={TRADES: self.trade}))
        self.fh.add_feed(BinanceFutures(symbols=self.markets['binance-futures'], channels=[L2_BOOK], callbacks={L2_BOOK: self.book}))
        
        self.fh.run()
    
        
    async def trade(self, t, receipt_timestamp):
        assert isinstance(t.timestamp, float)
        assert isinstance(t.side, str)
        assert isinstance(t.amount, Decimal)
        assert isinstance(t.price, Decimal)
        assert isinstance(t.exchange, str)
        #print(f"Trade received at {receipt_timestamp}: {t}")
        symbol = self.fix_market_names(t.symbol)

        lname = 'trade_' + symbol
        #timestamp, side, amount, price
        #trade = [receipt_timestamp, t.timestamp, symbol, t.side, t.amount, t.price]
        trade = {
            'receipt_timestamp': receipt_timestamp,
            'trade_timestamp': t.timestamp,
            'symbol': symbol,
            'side': t.side,
            'amount': t.amount,
            'price': t.price
        }
        trade = json.dumps(trade, cls=DecimalEncoder)
        #print(datetime.now(), 'trade', trade)
        # Push the list of dictionaries into the Redis list
        redis_client.rpush(lname, trade)
        if redis_client.llen(lname) > self.max_length:
            redis_client.ltrim(lname, -self.max_length, - 1)
        

    async def book(self, book, receipt_timestamp):
        #print(f'Book received at {receipt_timestamp} for {book.exchange} - {book.symbol}, with {len(book.book)} entries. Top of book prices: {book.book.asks.index(0)[0]} - {book.book.bids.index(0)[0]}')
        symbol = self.fix_market_names(book.symbol)
        best_bid_price = book.book.bids.index(0)[0]
        best_bid_size = book.book.bids.index(0)[1]
        best_ask_price = book.book.asks.index(0)[0]
        best_ask_size = book.book.asks.index(0)[1]
        snap = [receipt_timestamp, symbol, best_bid_price,best_bid_size, \
            best_ask_price, best_ask_size]
        
        snap = {
            'receipt_timestamp': receipt_timestamp,
            'symbol': symbol,
            'best_bid_price': best_bid_price,
            'best_bid_size': best_bid_size,
            'best_ask_price': best_ask_price,
            'best_ask_size': best_ask_size
        }
        snap = json.dumps(snap, cls=DecimalEncoder)
        #print(datetime.now(), 'snap', snap)
        
        
        lname = 'ob_' + symbol
        # Push the list of dictionaries into the Redis list
        redis_client.rpush(lname, snap)
        if redis_client.llen(lname) > self.max_length:
            redis_client.ltrim(lname, -self.max_length, - 1)
        
        # Publish data to a channel named 'data_channel'
        if self.check_book_overlapping(book):
            self.fix_book_overlapping(book)

    def fix_book_overlapping(self, book):
        ask = book.book.asks.index(0)[0]
        del book.book.asks[ask]
        bid = book.book.bids.index(0)[0]
        del book.book.bids[bid]

    def check_book_overlapping(self, book):
        if book.book.bids.index(0) > book.book.asks.index(0):
            return True
    
if __name__ == '__main__':
    feed = Connector()
    feed.run()