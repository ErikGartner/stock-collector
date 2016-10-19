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

class YahooRealTime(Source):

    def __init__(self, mongo):
        super().__init__('YahooRealTime', mongo)

    def _download_data(self, symbols, params):

        params = {
            'q': YQL_QUERY % ','.join(symbols),
            'format': 'json',
            'env': 'store://datatables.org/alltableswithkeys',
            'callback': ''
        }

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

        query = r.json()['query']
        results = query['results']['quote']
        time = query['created']

        data = []
        for r in results:
            d = {
                'source': self.name,
                'time': time,
                'data': r,
                'ticker': r['symbol']
            }
            data.append(d)
        return data
