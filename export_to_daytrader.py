from datetime import datetime, timedelta
import json
import os

from pymongo import MongoClient
import pandas as pd
from slugify import slugify
import yaml
import pytz

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
    for i in reversed(range(1, len(day_points))):
        day_points[i]['Volume'] = int(day_points[i]['Volume'])
        day_points[i - 1]['Volume'] = int(day_points[i - 1]['Volume'])
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
        point['time'] = pytz.timezone('Europe/Stockholm').localize(d['time'] + timedelta(hours=1))
        new_points.append(point)

    # Adjust volume
    adjust_volume(new_points)

    df = pd.DataFrame.from_dict(new_points, orient='columns', dtype=float)
    df = df[['time', 'LastTradePriceOnly', 'Volume']]
    df = df.rename(index=str, columns={'LastTradePriceOnly': 'price', 'Volume': 'nbr'})
    df = df.set_index('time')
    new_data = pd.DataFrame(df[-1:].values, index=[df.index[-1] + timedelta(minutes=10)], columns=df.columns)
    df = df.append(new_data)
    return df


def verify_integrity(frame):
    if len(frame) != 41:
        print('Incorrect length', len(frame))
        print(frame)
        return False
    return True


def stock_with_frame_to_file(stock_info, trades_frame, base_path='data/',
                             date_folder=True):
    """
    Exports the data to a h5 file. The stock info is stored as a .yml.
    """
    # Create date folder
    if date_folder:
        date = trades_frame.index[0].date().isoformat()
        folder = os.path.join(base_path, date)
    else:
        folder = base_path

    os.makedirs(folder, exist_ok=True)

    # Create file name from stock name slug
    stock_name = slugify(stock_info['name'])
    file_name = os.path.join(folder, stock_name)

    # Save trades data
    trades_frame.to_hdf(file_name + '.h5', 'trade_frame', format='table',
                        mode='w')

    # Save information about stock
    with open(file_name + '.yml', 'w') as outfile:
        yaml.dump(stock_info, outfile)


def export_stock(ticker, from_date=None, to_date=None):
    if to_date is None:
        to_date = datetime.utcnow()
    if from_date is None:
        from_date = datetime(1970, 1, 1)

    config = read_config()
    client = MongoClient(config['mongo_uri'])
    data_db = client.get_default_database().stock_collector

    data = list(data_db.find({
        'ticker': ticker,
        'time': {'$gte': from_date, '$lte': to_date}
        }).sort('time'))

    return_list = []
    i = 0
    while i < len(data):
        day = []
        date = data[i]['time'].date()
        for j in range(len(data) - i):
            if data[i+j]['time'].date() == date:
                day.append(data[i+j])
            else:
                break
        frame = format_point(day)
        if verify_integrity(frame):
            stock_info = {
                'name': data[0]['data']['Name'],
                'stock_exchange': data[0]['data']['StockExchange'],
                'ticker': ticker,
                'date': date
            }
            return_list.append((stock_info, frame))
        i += j + 11

    return return_list


if __name__ == '__main__':
    config = read_config()

    for ticker in config['tickers']:
        for stock_info, frames in export_stock(ticker):
            stock_with_frame_to_file(stock_info, frames,
                                     base_path='/Users/erik/Desktop/data')
