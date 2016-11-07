import sys
import requests
import csv
import time
import pytz
import datetime
from tzlocal import get_localzone

from .source import Source

# Yahoo urls
YQL_URL = 'http://finance.yahoo.com/d/quotes.csv'

# API stability parameters
RETRIES = 10
RETRY_WAIT = 20 / 1000

# Data query keys (selection)
DATA_KEY_TABLE = {'Ask': 'a', 'AverageDailyVolume': 'a2', 'AskSize': 'a5',
                  'Bid': 'b', 'AskRealTime': 'b2', 'BidRealTime': 'b3',
                  'BidSize': 'b6', 'ChangeAndPercentChange': 'c',
                  'Change': 'c1', 'ChangeRealTime': 'c6',
                  'LastTradeDate': 'd1', 'TradeDate': 'd2', 'DaysLow': 'g',
                  'DaysHigh': 'h', '52WeekLow': 'j', '52WeekHigh': 'k',
                  'OrderBookRealTime': 'i5', 'MarketCapRealTime': 'j3',
                  'LastTradeWithTimeRealTime': 'k1',
                  'ChangePercentRealTime': 'k2', 'LastTradeSize': 'k3',
                  'LastTradeWithTime': 'l', 'LastTradePriceOnly': 'l1',
                  'Open': 'o', 'PreviousClose': 'p', 'PricePaid': 'p1',
                  'Name': 'n', 'ChangeInPercent': 'p2', 'Volume': 'v',
                  'PERatioRealTime': 'r2', 'Symbol': 's', 'StockExchange': 'x'}

# Set data keys to collect, Symbol and StockExchange required, Symbol at [0]
KEYS_TO_COLLECT = ['Symbol', 'StockExchange', 'Name', 'LastTradeWithTime',
                   'LastTradeDate', 'Ask', 'Bid', 'DaysLow', 'DaysHigh',
                   '52WeekLow', '52WeekHigh', 'LastTradeWithTimeRealTime',
                   'Volume', 'AverageDailyVolume', 'LastTradePriceOnly']

# StockExchange callsign : (market timezone, market open, market close)
MARKET_TIMES = {'STO': (pytz.timezone('Europe/Stockholm'),
                        datetime.time(9, 00), datetime.time(17, 30)),
                'SNP': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00)),
                'NMS': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00)),
                'CCY': (pytz.timezone('US/Eastern'),
                        datetime.time(17, 00), datetime.time(16, 00)),
                'NYM': (pytz.timezone('US/Eastern'),
                        datetime.time(18, 00), datetime.time(17, 15)),
                'CMX': (pytz.timezone('US/Eastern'),
                        datetime.time(18, 00), datetime.time(17, 15))}


class YahooRealTime(Source):

    def __init__(self, mongo):
        super().__init__('YahooRealTime', mongo)
        self.symbol_market = {}

    def _download_data(self, symbols, params):
        symbols = [s for s in symbols if
                   self._is_trading(self.symbol_market.get(s))]
        if len(symbols) == 0:
            return []

        # query parameters
        data_keys = [DATA_KEY_TABLE[k] for k in KEYS_TO_COLLECT]
        params = {
            's': '+'.join(symbols),
            'f': ''.join(data_keys)
        }

        # retry downloads
        for i in range(RETRIES):
            r = requests.get(YQL_URL, params=params)
            date = datetime.datetime.now()
            if r.status_code == 200:
                break
            else:
                time.sleep(RETRY_WAIT)
        else:
            print('Error while fetching %s\n%s' % (r.url, r.content),
                  file=sys.stderr)
            return []

        # parse data
        decoded_content = r.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        data_list = list(cr)

        # build data
        tz = get_localzone()
        data = []
        for stock in data_list:
            d = {
                'source': self.name + ' csv',
                'time': (tz.normalize(tz.localize(date)).astimezone(pytz.utc)
                         .strftime('%Y-%m-%dT%H:%M:%SZ')),
                'ticker': stock[0],
                'data': dict(zip(KEYS_TO_COLLECT[1:], stock[1:]))
            }

            # add missing symbol market data
            ticker = d['ticker']
            ticker_market = d['data']['StockExchange']
            if ticker not in self.symbol_market:
                self.symbol_market[ticker] = ticker_market
                if self._is_trading(self.symbol_market.get(ticker)):
                    data.append(d)
            else:
                data.append(d)
        return data

    def _is_trading(self, market):
        if market not in MARKET_TIMES:
            if market is not None:
                print('No user added time data for market \'%s\'.' % market)
            return True

        d = datetime.datetime.now()
        tz = get_localzone()
        utc_time = tz.normalize(tz.localize(d)).astimezone(pytz.utc)

        market_info = MARKET_TIMES[market]
        market_time = utc_time.astimezone(market_info[0])

        # Currency/some commodity markets are open Sunday - Friday
        if market in {'CCY', 'CMX', 'NYM'}:
            if ((market_time.isoweekday() == 7 and
                 market_time.time() >= market_info[1]) or
                (market_time.isoweekday() == 5 and
                 market_time.time() <= market_info[2]) or
                (market_time.isoweekday() < 5)):
                return True
            else:
                return False

        return (market_time.time() >= market_info[1] and
                market_time.time() <= market_info[2] and
                market_time.isoweekday() in range(1, 6))
