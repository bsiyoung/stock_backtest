import os
from datetime import datetime, timedelta
import pickle
import time

import numpy as np
import pandas as pd
import yfinance as yf


class DataManager:
    def __init__(self, cache_path):
        self.cache_path = os.path.abspath(cache_path)

    def update(self, tickers, force_update=False, update_freq=80000, verbose=False):
        if type(tickers) is str:
            tickers = [tickers]

        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)

        start_date = '1900-01-01'
        end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

        for i, ticker in enumerate(tickers):
            file_name = '{}.cache'.format(ticker)
            file_path = os.path.join(self.cache_path, file_name)

            if os.path.isfile(file_path) and force_update is False:
                # Check file modification time
                if time.time() - os.path.getmtime(file_path) < update_freq:
                    if verbose:
                        print('({}/{}) [{}] Pass'.format(i + 1, len(tickers), ticker))
                    continue

            data = yf.download(ticker, start_date, end_date, progress=False)
            if len(data) == 0:
                if verbose:
                    print('({}/{}) [{}] Empty data'.format(i + 1, len(tickers), ticker))
                continue

            adj_mult = data['Adj Close'] / data['Close']
            data.insert(4, 'Adj Open', data['Open'] * adj_mult)
            data.insert(5, 'Adj High', data['High'] * adj_mult)
            data.insert(6, 'Adj Low', data['Low'] * adj_mult)

            wf = open(file_path, 'wb')
            pickle.dump(data, wf)
            wf.close()

            if verbose:
                print('({}/{}) [{}] Updated'.format(i + 1, len(tickers), ticker))

    def acc_update(self, tickers, sz_acc_list=None, force_update=False, update_freq=80000, verbose=False):
        if type(tickers) is str:
            tickers = [tickers]

        if sz_acc_list is None:
            sz_acc_list = [1, 5, 30]

        datas = self.get(tickers)

        for i, ticker in enumerate(list(datas.keys())):
            file_name = '{}_acc.cache'.format(ticker)
            file_path = os.path.join(self.cache_path, file_name)

            if os.path.isfile(file_path) and force_update is False:
                if time.time() - os.path.getmtime(file_path) < update_freq:
                    if verbose:
                        print('({}/{}) [{}](Acc) Pass'.format(i + 1, len(tickers), ticker))

                    continue

            data = datas[ticker]
            column_names = data.columns.values.tolist()
            column_names.append('Date')

            res = dict()
            for sz_acc in sz_acc_list:
                acc_df = pd.DataFrame(columns=column_names)
                acc_df.set_index('Date', inplace=True)

                if verbose:
                    print('({}/{}) [{}](Acc) sz_acc={}'.format(i + 1, len(tickers), ticker, sz_acc))

                for idx in range(sz_acc - 1, len(data)):
                    chunk = data[idx-sz_acc+1:idx+1]
                    row_data = {
                        'Open': chunk['Open'].values[0],
                        'High': max(chunk['High'].values),
                        'Low': min(chunk['Low'].values),
                        'Close': chunk['Close'].values[-1],
                        'Adj Open': chunk['Adj Open'].values[0],
                        'Adj High': max(chunk['Adj High'].values),
                        'Adj Low': min(chunk['Adj Low'].values),
                        'Adj Close': chunk['Adj Close'].values[-1],
                        'Volume': np.sum(chunk['Volume'].values)
                    }

                    curr_date = chunk.index.values[-1]
                    acc_df.loc[curr_date] = row_data

                    if idx % 100 == 0 or idx == len(data) - 1:
                        n_total_data = len(data) - sz_acc + 1
                        curr_data = idx - sz_acc + 2
                        progress = round(curr_data / n_total_data * 100, 2)
                        print('\r{}%'.format(progress), end='')

                if verbose:
                    print()

                res[sz_acc] = acc_df

            wf = open(file_path, 'wb')
            pickle.dump(res, wf)
            wf.close()

            if verbose:
                print()

    def delete(self, tickers):
        if type(tickers) is str:
            tickers = [tickers]

        for ticker in tickers:
            file_name = '{}.cache'.format(ticker)
            file_path = os.path.join(self.cache_path, file_name)

            if os.path.isfile(file_path):
                os.remove(file_path)

    def get(self, tickers, acc=False):
        if type(tickers) is str:
            tickers = [tickers]

        res = dict()
        for ticker in tickers:
            file_name = '{}{}.cache'.format(ticker, '_acc' if acc else '')
            file_path = os.path.join(self.cache_path, file_name)

            if not os.path.isfile(file_path):
                continue

            rf = open(file_path, 'rb')
            res[ticker] = pickle.load(rf)
            rf.close()

        return res

    @staticmethod
    def match_date(datas):
        data_buf = []
        for ticker in list(datas.keys()):
            for sz_acc in list(datas[ticker].keys()):
                data_buf.append(datas[ticker][sz_acc])

        if len(data_buf) <= 1:
            return

        intersect_index = set(data_buf[0].index)
        for data in data_buf[1:]:
            intersect_index = intersect_index.intersection(set(data.index))

        for i in range(len(data_buf)):
            cmp_idx = list(set(data_buf[i].index) - intersect_index)
            for idx in cmp_idx:
                data_buf[i].drop(idx, inplace=True)

    @staticmethod
    def get_datetime_range(data):
        if type(data) is dict:
            ticker = list(data.keys())[0]
            sz_acc = list(data[ticker].keys())[0]
            data = data[ticker][sz_acc]

        return data.index[0], data.index[-1]


def test():
    dm = DataManager('./cache')

    tickers = ['^IXIC', 'QQQ', 'QLD', 'TQQQ', 'PSQ', 'QID', 'SQQQ']
    dm.update(tickers, verbose=True)
    print()
    dm.acc_update(tickers, sz_acc_list=[1, 5, 30], verbose=True)
    acc = dm.get('QQQ', acc=True)
    dm.match_date(acc)
    print(acc)


if __name__ == '__main__':
    test()
