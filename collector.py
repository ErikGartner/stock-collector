import time
import json
import os
from datetime import datetime, timezone

from pymongo import MongoClient

from sources.yahoo import YahooRealTime


def read_config():
    """
    Read the config for this module.
    Priority is env, config.json then defaults.
    """

    # 3. Defaults. All items should have keys here.
    config = {
        'mongo_uri': 'mongodb://localhost/stock-data',
        'interval': 600,
        'tickers': []
    }

    # 2. Read from config.json
    with open('config.json') as data_file:
        config.update(json.load(data_file))

    # 1. Environment
    for key in config:
        val = os.getenv(key, None)
        if val is not None:
            config[key] = val

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
