import sys
import requests
import time

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
