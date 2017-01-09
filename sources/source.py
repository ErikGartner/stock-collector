import time
from datetime import datetime, timedelta
import pytz


GAP_CONSTANT = timedelta(minutes=5)


class Source:

    def __init__(self, name, data_db, metadata_db, config):
        self.name = name
        self.data_db = data_db
        self.metadata_db = metadata_db
        self.config = config
        self.timeout = GAP_CONSTANT + timedelta(seconds=config['interval'])

    def download_data(self, symbols, params=None):
        print('%s - downloading %s' % (self.name, symbols))
        fetch_time = datetime.now().replace(tzinfo=pytz.utc)

        data = {}
        for i in range(0, len(symbols), 20):
            data.update(self._download_data(symbols[i:i+20], params))

            # Be nice to the api
            time.sleep(50 / 1000)

        # Only add data to collection if we have collected data
        self._update_data(data, fetch_time)
        print('%s - done!' % self.name)

    def _download_data(self, symbols, params):
        pass

    def _update_data(self, data, fetch_time):
        insert_list = []
        for ticker, values in data.items():
            if values is False:
                print('Failed to download %s' % ticker)
            else:
                ticker_metadata = self.metadata_db.find_one({
                    'ticker': ticker,
                    'source': self.name,
                    'interval': self.config['interval']},
                    sort=[('time', -1)])

                if (ticker_metadata is None or
                    (fetch_time - ticker_metadata['end'].
                     replace(tzinfo=pytz.UTC) > self.timeout)):

                    self.metadata_db.insert({
                        'ticker': ticker,
                        'source': self.name,
                        'start': fetch_time,
                        'end': fetch_time,
                        'interval': self.config['interval']
                    })
                else:
                    ticker_metadata['end'] = fetch_time
                    self.metadata_db.update({'_id': ticker_metadata['_id']},
                                            ticker_metadata)

                # values is True if succesfully updated but no new data
                if values is not True:
                    insert_list.append(values)
        self.data_db.insert_many(insert_list)
