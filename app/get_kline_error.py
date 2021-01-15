#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2021-01-15 10:16
# @Author  : NingAnMe <ninganme@qq.com>
from binance_f import RequestClient
from binance_f.model import *
from binance_f.constant.test import *

from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from pprint import pprint

import requests


def str2datetime(dt_str: str):
    return datetime.strptime(dt_str, '%Y-%m-%d-%H-%M-%S')


def str2timestamp(dt_str: str):
    dt = str2datetime(dt_str)
    return str(dt.timestamp() * 1000)[:13]


request_client = RequestClient(api_key=g_api_key, secret_key=g_secret_key)

pair = "BTCUSDT"
contractType = ContractType.PERPETUAL
interval = CandlestickInterval.DAY1
startTime = str2timestamp('2020-12-01-00-00-00')
endTime = str2timestamp('2021-01-14-00-00-00')
limit = 1500

request = request_client.request_impl.get_continuous_candlestick_data(pair, contractType, interval, startTime, endTime, limit)

print(request.host + request.url)
proxies = {"http": "http://127.0.0.1:1083", "https": "http://127.0.0.1:1083"}
response = requests.get(request.host + request.url, headers=request.header, proxies=proxies)

pprint(response.json())
