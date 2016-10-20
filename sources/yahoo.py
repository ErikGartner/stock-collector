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


class YahooRealTime(Source):

    def __init__(self, mongo):
        super().__init__('YahooRealTime', mongo)

    def _download_data(self, symbols, params):
        symbols = [s for s in symbols if self._is_trading(s)]
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
            data.append(d)
        return data

    def _is_trading(self, symbol):
        # Currencies and commodities
        if '=' in symbol:
            return True

        d = datetime.datetime.now()
        tz = get_localzone()
        utc_time = tz.normalize(tz.localize(d)).astimezone(pytz.utc)

        # Stocks

        # United States per default
        symbol_timezone = pytz.timezone('US/Eastern')
        symbol_open = datetime.time(9, 30)
        symbol_close = datetime.time(16, 00)

        # Swedish
        if '.ST' in symbol or '^OMX' in symbol:
            symbol_timezone = pytz.timezone('Europe/Stockholm')
            symbol_open = datetime.time(9, 00)
            symbol_close = datetime.time(17, 30)

        symbol_time = utc_time.astimezone(symbol_timezone)
        return (symbol_time.time() > symbol_open and
                symbol_time.time() < symbol_close and
                symbol_time.isoweekday() in range(1, 5))
