import time
import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient

from sources.yahoo import YahooRealTime


def read_config():

    with open('config.json') as data_file:
        config = json.load(data_file)

    if 'mongo_uri' not in config:
        config['mongo_uri'] = os.environ.get('mongo_uri',
                                             'mongodb://localhost:3001/meteor')

    return config


def sleep_tracker(interval):
    """
    This method ensure that all fetches occurs at predictable times.
    For example if interval is 10 min, calls will aways be 12.00, 12.10, 12.20
    etc regardless of the exact moment when the program started.
    """

    # Set a defined epoch independent of machine epoch
    epoch = datetime(2000, 1, 1, hour=0, minute=0, second=0,
                     microsecond=0, tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    diff = (now_utc - epoch).total_seconds()
    time_elasped = diff % interval
    sleep_time = interval - time_elasped
    return sleep_time


if __name__ == '__main__':

    config = read_config()

    client = MongoClient(config['mongo_uri'])
    data_db = client.get_default_database().stock_collector
    metadata_db = client.get_default_database().stock_collector_metadata
    source = YahooRealTime(data_db, metadata_db, config)

    while True:
        time.sleep(sleep_tracker(config['interval']))
        source.download_data(config['tickers'])
