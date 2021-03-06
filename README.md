# Stock Collector
**ABANDONED AFTER YAHOO SHUTDOWN THEIR API**

*Downloads available real time data for a set of instruments and stores it in a Mongo database*

This small python modules enables the user to download and store near realtime Yahoo Finance stock, to be viewed and used later for private purposes.

The program collects realtime data with predefined intervals. It also stores metadata to make it easier to detect gaps in data due to failures. The metadata displays the data as continuous and complete as long as either the data was successfully downloaded or there was no available data (due to markets being closed).

## Usage
Built for Python3.5 using requests and pymongo.

1. Install requirements in `requirements.txt`.
2. Configure using `config.json` and set `mongo_uri` in environment.
3. Run `python collector.py`

### config.json
```json
{
  "tickers": [
    "^OMX",
    "AAPL"
  ],
  "interval": 600
}
```

### Mongo Format
Data:
```python
{  
   "_id":ObjectId("586be2b45872efc3d1f311f4"),
   "source":"YahooRealTime",
   "time":   ISODate("2017-01-03T18:43:10.345   Z"),
   "ticker":"GOOGL",
   "data":{  
      "StockExchange":"NMS",
      "Name":"Alphabet Inc.",
      "LastTradeWithTime":"12:27pm - <b>808.31</b>",
      "LastTradeDate":"1/3/2017",
      "Ask":"808.19",
      "Bid":"807.81",
      "DaysLow":"796.89",
      "DaysHigh":"811.43",
      "52WeekLow":"672.66",
      "52WeekHigh":"839.00",
      "LastTradeWithTimeRealTime":"N/A",
      "Volume":"1011528",
      "AverageDailyVolume":"1871150",
      "LastTradePriceOnly":"808.31"
   }
}
```
Metadata:
```python
{  
   "_id":ObjectId("5873c89cecb91b00083f13ea"),
   "interval":600,
   "start":   ISODate("2017-01-09T17:30:00.103   Z"),
   "source":"YahooRealTime",
   "end":   ISODate("2017-01-09T17:40:00.103   Z"),
   "ticker":"BONAV-B.ST"
}
```

## Data License
Please read and adhere to the [Yahoo! APIs Terms of Use](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm). This module is not developed or affiliated with Yahoo! Inc.

## Module License
The MIT License (MIT)

Copyright (c) 2016 Erik Gärtner
