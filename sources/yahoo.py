import sys
import requests
import time
import pytz
import datetime
from tzlocal import get_localzone

from .source import Source

# Yahoo urls
YQL_URL = 'https://query.yahooapis.com/v1/public/yql'
YQL_QUERY = 'select * from yahoo.finance.quotes where symbol in ("%s")'

# API stability parameters
RETRIES = 10
RETRY_WAIT = 20 / 1000

# query keys
DATA_KEYS = ['Currency', 'LastTradeDate', 'LastTradeWithTime', 'Name',
             'PreviousClose', 'Symbol', 'StockExchange', 'Ask',
             'AverageDailyVolume', 'Bid', 'BookValue', 'Change',
             'DividendShare', 'EPSEstimateCurrentYear', 'EPSEstimateNextYear',
             'EPSEstimateNextQuarter', 'DaysLow', 'DaysHigh', 'YearLow',
             'YearHigh', 'MarketCapitalization', 'EBITDA',
             'LastTradePriceOnly', 'Name', 'Open', 'DividendYield',
             'YearRange', 'PriceSales', 'PriceBook', 'PercentChange',
             'PercentChangeFromYearLow', 'PercentChangeFromYearHigh']

# StockExchange callsign : (market timezone, market open, market close)
MARKET_TIMES = {'STO': (pytz.timezone('Europe/Stockholm'),
                        datetime.time(9, 00), datetime.time(17, 30)),
                'SNP': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00)),
                'NMS': (pytz.timezone('US/Eastern'),
                        datetime.time(9, 30), datetime.time(16, 00))}

# Currency and commodity markets are always trading
ALWAYS_OPEN_MARKETS = ['CCY', 'CMX', 'NYM', 'CBT']


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
            'q': YQL_QUERY % ','.join(symbols),
            'format': 'json',
            'env': 'store://datatables.org/alltableswithkeys',
            'callback': ''
        }

        # retry downloads
        for i in range(RETRIES):
            r = requests.get(YQL_URL, params=params)
            if r.status_code == 200:
                break
            else:
                time.sleep(RETRY_WAIT)
        else:
            print('Error while fetching %s\n%s' % (r.url, r.content),
                  file=sys.stderr)
            return []

        # parse data
        query = r.json()['query']
        results = query['results']['quote']
        time = query['created']

        # build data
        data = []
        for r in results:
            d = {
                'source': self.name,
                'time': time,
                'data': {key: r[key] for key in DATA_KEYS if key in r},
                'ticker': r['symbol']
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
        if market in ALWAYS_OPEN_MARKETS:
            return True

        if market not in MARKET_TIMES:
            if market is not None:
                print('No user added time data for market \'%s\'.' % market)
            return True

        d = datetime.datetime.now()
        tz = get_localzone()
        utc_time = tz.normalize(tz.localize(d)).astimezone(pytz.utc)

        market_info = MARKET_TIMES[market]
        market_time = utc_time.astimezone(market_info[0])
        return (market_time.time() >= market_info[1] and
                market_time.time() <= market_info[2] and
                market_time.isoweekday() in range(1, 6))
