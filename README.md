# Stock Collector
*Downloads available real time data for a set of instruments and stores it in a Mongo database*

This small python modules enables the user to download and store near realtime Yahoo Finance stock, to be viewed and used later for private purposes.

## Usage
Built for Python3.5 using requests and pymongo.

1. Install requirements in `requirements.txt`.
2. Configure using `config.json` and set `MONGO_URL` in environment.
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

## Data License
Please read and adhere to the [Yahoo! APIs Terms of Use](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm). This module is not developed or affiliated with Yahoo! Inc.

## Module License
The MIT License (MIT)

Copyright (c) 2016 Erik Gärtner
