from binance_f import RequestClient
from binance_f.model import *
from binance_f.constant.test import *
from binance_f.constant.system import Proxies

from datetime import datetime
from dateutil.relativedelta import relativedelta

import requests
import pandas as pd
from pprint import pprint

RECORDS_DIR = 'records'
CACHE_DIR = 'cache'

START_TIME = {
    'BTCUSDT.BINANCE': '2019-09-02'
}


def str2datetime(dt_str: str):
    return datetime.strptime(dt_str, '%Y-%m-%d-%H-%M-%S')


def str2timestamp(dt_str: str):
    dt = str2datetime(dt_str)
    return str(dt.timestamp() * 1000)[:13]


def datetime2timestamp(dt: datetime):
    return str(dt.timestamp() * 1000)[:13]


def datetime2ymd(dt: datetime):
    return dt.strftime("%Y-%m-%d")


def timestamp2datetime(ts: str):
    return datetime.fromtimestamp(float(ts) / 1000)


def object2dict(obj):
    model_dict = dict(obj.__dict__)
    return model_dict


def dict_add_datetime_open_interest(d):
    d['datetime'] = datetime.fromtimestamp(float(d['openTime']) / 1000)
    d['open_interest'] = 0
    return d


def split_datetime(dt_start: datetime, dt_end: datetime, interval: str = 'm'):
    dts = list()
    if 'm' in interval:
        limit = relativedelta(days=1) - relativedelta(minutes=1)
        step = relativedelta(days=1)
    elif 'h' in interval:
        limit = relativedelta(months=1) - relativedelta(hours=1)
        step = relativedelta(months=1)
    elif 'd' in interval:
        limit = relativedelta(years=1) - relativedelta(days=1)
        step = relativedelta(years=1)
    else:
        raise ValueError(interval)

    while (dt_start + limit) < dt_end:
        dts.append((dt_start, dt_start + limit))
        dt_start += step
    if dt_start <= dt_end:
        dts.append((dt_start, dt_end))
    return dts


def get_pairs_usdt(request_client):
    request = request_client.request_impl.get_exchange_information()
    exchange_info = requests.get(request.host + request.url, headers=request.header, proxies=Proxies.proxies).json()
    pairs = list()
    for symbol in exchange_info['symbols']:
        if symbol.get('quoteAsset') == 'USDT' and symbol.get('quoteAsset') == 'USDT' and \
                symbol.get('contractType') == 'PERPETUAL':
            pairs.append((symbol['pair'], symbol['onboardDate']))
            # if symbol['pair'] == 'BTCUSDT':
            # pprint(symbol)
    pprint(pairs)
    return pairs


def combine_records(records_files, out_file):
    print(len(records_files))
    result = None
    for f in records_files:
        try:
            df = pd.read_csv(f, index_col=False)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        if result is None:
            result = df
        else:
            result = pd.concat([result, df])
    if result is not None and not result.empty:
        print(len(result))
        result.drop_duplicates(inplace=True)
        result.to_csv(out_file, index=False)
        result.sort_values(by=['openTime'])
        print('>>> {}'.format(out_file))


def main():
    request_client = RequestClient(api_key=g_api_key, secret_key=g_secret_key)

    exchange = 'BINANCE'
    startDatetime = datetime(2019, 9, 1)
    endDatetime = datetime(2021, 1, 13)
    pairs = [('BTCUSDT', None)]
    intervals = [CandlestickInterval.DAY1]
    contractType = ContractType.PERPETUAL
    limit = 1500

    dt_yes = datetime.now() - relativedelta(days=1)
    endDatetime = datetime(dt_yes.year, dt_yes.month, dt_yes.day, 23, 59)
    pairs = get_pairs_usdt(request_client)
    intervals = [CandlestickInterval.DAY1, CandlestickInterval.HOUR1, CandlestickInterval.MIN1]

    for pair, timestamp in pairs:
        for interval in intervals:
            if timestamp is not None:
                onboardDate = timestamp2datetime(timestamp)
                startDatetime = datetime(onboardDate.year, onboardDate.month, 1)
            # print(pair, interval, startDatetime, endDatetime)
            dts = split_datetime(startDatetime, endDatetime, interval)
            out_filename = '{}_{}_{}_{}_{}_{}_{}.csv'.format(exchange, pair, contractType, interval,
                                                             datetime2ymd(startDatetime),
                                                             datetime2ymd(endDatetime),
                                                             limit)
            out_dir = os.path.join(RECORDS_DIR, datetime2ymd(endDatetime))
            if not os.path.isdir(out_dir):
                os.makedirs(out_dir)
            out_file_record = os.path.join(out_dir, out_filename)
            if os.path.isfile(out_file_record):
                print('{}'.format(out_file_record))
                continue

            records_files = []

            for dt_start, dt_end in dts:
                # print(dt_start, dt_end)
                startTime = datetime2timestamp(dt_start)
                endTime = datetime2timestamp(dt_end)
                out_filename = '{}_{}_{}_{}_{}_{}_{}.csv'.format(exchange, pair, contractType, interval,
                                                                 datetime2ymd(dt_start), datetime2ymd(dt_end), limit)
                out_dir = os.path.join(CACHE_DIR, interval)
                if not os.path.isdir(out_dir):
                    os.makedirs(out_dir)
                out_file_cache = os.path.join(out_dir, out_filename)
                if os.path.isfile(out_file_cache):
                    records_files.append(out_file_cache)
                    continue

                result = request_client.get_continuous_candlestick_data(pair=pair, contractType=contractType,
                                                                        interval=interval,
                                                                        startTime=startTime, endTime=endTime,
                                                                        limit=limit)
                records = [dict_add_datetime_open_interest(object2dict(i)) for i in result]
                df = pd.DataFrame.from_records(records)
                print(df)

                df.to_csv(out_file_cache, index=False)
                print('>>> {}'.format(out_file_cache))
                print(request_client.limits)
                records_files.append(out_file_cache)

            combine_records(records_files, out_file_record)


if __name__ == '__main__':
    while True:
        try:
            main()
        except requests.exceptions.ConnectionError as why:
            print(why)
