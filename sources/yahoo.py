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
# a Ask, a2 Average Daily Volume, a5 Ask Size
# b Bid, b2 Ask (Real-time), b3 Bid (Real-time)
# b6 Bid Size, c Change & Percent Change
# c1 Change, c6 Change (Real-time), d1 Last Trade Date, d2 Trade Date
# g Day’s Low, h Day’s High, j 52-week Low, k 52-week High
# i5 Order Book (Real-time),j yearLow, k yearHigh, j3 Market Cap (Real-time)
# k1 Last Trade (Real-time) With Time
# k2 Change Percent (Real-time), k3 Last Trade Size
# l Last Trade (With Time), l1 Last Trade (Price Only)
# o Open, p Previous Close, p1 Price Paid, n Name
# p2 Change in Percent, r2 P/E Ratio (Real-time), s Symbol, x Stock exchange
DATA_KEYS = 'snll1d1abghjkk1va2x'

# StockExchange callsign : (market timezone, market open, market close)
MARKET_TIMES = {'STO': (pytz.timezone('Europe/Stockholm'),
                        datetime.time(9, 00), datetime.time(17, 30)),
                'SNP': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00)),
                'NMS': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00)),
                'CCY': (pytz.timezone('US/Eastern'),
                        datetime.time(17, 00), datetime.time(16, 00))}


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
        params = {
            's': '+'.join(symbols),
            'f': DATA_KEYS
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
                # Make sure you match the order with your parameter signature
                'data': {'Name': stock[1],
                         'LastTradeWithTime': stock[2],
                         'LastTradePriceOnly': stock[3],
                         'LastTradeDate': stock[4],
                         'Ask': stock[5],
                         'Bid': stock[6],
                         'DaysLow': stock[7],
                         'DaysHigh': stock[8],
                         'YearLow': stock[9],
                         'YearHigh': stock[10],
                         'LastTradeWithTimeRealTime': stock[11],
                         'Volume': stock[12],
                         'AverageDailyVolume': stock[13],
                         'StockExchange': stock[14]
                         }
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

        # Currency markets are open (ET) Sunday 1700-Friday 1600
        if market == 'CCY':
            if ((market_time.isoweekday() == 8 and
                 market_time.time() >= market_info[1]) or
                (market_time.isoweekday() == 6 and
                 market_time.time() <= market_info[2]) or
                (market_time.isoweekday() < 6)):
                return True
            else:
                return False

        return (market_time.time() >= market_info[1] and
                market_time.time() <= market_info[2] and
                market_time.isoweekday() in range(1, 6))
