from datetime import datetime

from pymongo import MongoClient
import pandas as pd

from collector import read_config

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


def adjust_volume(day_points):
    """
    Adjusts a list of cummulative volumes to incremental.
    """
    for i in reversed(range(1, len(day_ticks))):
        day_points[i]['Volume'] -= day_points[i - 1]['Volume']
    return day_points

def format_point(day_points):
    """
    Format points to daytrader formet.
    """
    # Get data and time
    new_points = []
    for d in day_points:
        point = d['data']
        point['time'] = d['time']
        new_points.append(point)

    # Adjust volume
    adjust_volume(new_points)

    df = pd.DataFrame.from_dict(new_points, orient='columns', dtype=float)
    df = df['time', 'LastTradePriceOnly', 'Volume'].rename(index='time',
                                                           {'LastTradePriceOnly': 'price', 'Volume': 'nbr'})
    return df

def export_stock(ticker, from_date=None, to_date=None):
    if to_date is None:
        to_date = datetime.now()
    if from_date is None:
        from_date = datetime(1970,1,1)

    config = read_config()
    client = MongoClient(config['mongo_uri'])
    data_db = client.get_default_database().stock_collector

    data = list(data_db.find({
        'ticker': ticker,
        'time': {$elemMatch: {$gte: from_date, $lte: to_date}}
        }, {'time': 1}))

    adjusted_data = []
    i = 0
    while i < len(data):
        day = []
        date = data[i]['time'].date()
        for j in range(i, len(data)):
            if data[i+j]['time'].date() == date:
                day.append(data[i+j])
            else:
                break
        adjusted_data.append(format_point(day))
        i = j + 1
    return adjusted_data
