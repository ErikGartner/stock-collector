import sys
import requests

from .source import Source

# Yahoo urls
YQL_URL = 'https://query.yahooapis.com/v1/public/yql'
YQL_QUERY = 'select * from yahoo.finance.quotes where symbol in ("%s")'


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

        r = requests.get(YQL_URL, params=params)
        if r.status_code != 200:
            print('Error while fetching %s' % r.url, file=sys.stderr)
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
