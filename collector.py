import time
import json
import os

from pymongo import MongoClient

from sources.yahoo import YahooRealTime


def read_config():

    with open('config.json') as data_file:
        config = json.load(data_file)

    if 'MONGO_URL' not in config:
        config['mongo_uri'] = os.getenv('MONGO_URL',
                                        'mongodb://localhost:3000/stock-data')

    return config


if __name__ == '__main__':

    config = read_config()

    client = MongoClient(config['mongo_uri'])
    collection = client.get_default_database().stock_collector
    source = YahooRealTime(collection)

    while True:
        source.download_data(config['tickers'])
        time.sleep(config['interval'])
