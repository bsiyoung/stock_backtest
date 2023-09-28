import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import pickle

import numpy as np
import pandas as pd
import yfinance as yf


class EmptyData(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class DataManager:
    def __init__(self, root_dir='.'):
        self.root = os.path.abspath(root_dir)
        self.cache_dir = os.path.join(self.root, 'cache')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

    def get(self, ticker, acc=False):
        cache_file = '{}{}.cache'.format(ticker, '_acc' if acc else '')
        cache_path = os.path.join(self.cache_dir, cache_file)

        if not os.path.isfile(cache_path):
            return None

        rf = open(cache_path, 'rb')
        data = pickle.load(rf)

        return data

    def update(self, tickers, progress=True, force_update=False, update_dt=86400):
        if type(tickers) is str:
            tickers = [tickers]

        end_date = datetime.now() + timedelta(days=1)
        for idx, ticker in enumerate(tickers):
            cache_file = '{}.cache'.format(ticker)
            cache_path = os.path.join(self.cache_dir, cache_file)

            if os.path.isfile(cache_path) and force_update is False:
                m_time = os.path.getmtime(cache_path)

                # Don't update if data is recently updated
                if (time.time() - m_time) <= update_dt:
                    if progress is True:
                        print('({}/{}) [{}] Recently updated'.format(idx + 1, len(tickers), ticker))
                    continue

            data = yf.download(ticker,
                               '1950-01-01', end_date.strftime("%Y-%m-%d"),
                               progress=False)
            if len(data) == 0:
                if progress is True:
                    print('({}/{}) [{}] Received empty data'.format(idx + 1, len(tickers), ticker))
                continue

            # Insert adjusted data columns
            adj_open = data['Open'].values / data['Close'].values * data['Adj Close']
            data.insert(4, 'Adj Open', adj_open)

            adj_high = data['High'].values / data['Close'].values * data['Adj Close']
            data.insert(5, 'Adj High', adj_high)

            adj_low = data['Low'].values / data['Close'].values * data['Adj Close']
            data.insert(6, 'Adj Low', adj_low)

            # Save data
            if not os.path.exists(self.cache_dir):
                os.mkdir(self.cache_dir)

            fw = open(cache_path, 'wb')
            pickle.dump(data, fw)

            if progress is True:
                print('({}/{}) [{}] Data updated'.format(idx + 1, len(tickers), ticker))

    def delete(self, ticker):
        cache_file = '{}.cache'.format(ticker)
        cache_path = os.path.join(self.cache_dir, cache_file)

        if os.path.isfile(cache_path):
            os.remove(cache_path)

    @staticmethod
    def match(dfs):
        # Get start and end datetime
        st_tm = None
        en_tm = None

        for df in dfs:
            if st_tm is None:
                st_tm = df.index[0]
            elif st_tm < df.index[0]:
                st_tm = df.index[0]

            if en_tm is None:
                en_tm = df.index[-1]
            elif en_tm > df.index[-1]:
                en_tm = df.index[0]

        # Cut dataframes with st_tm and en_tm
        for i, df in enumerate(dfs):
            st_idx = 0
            while df.index[st_idx] < st_tm and st_idx < len(df) - 1:
                st_idx += 1

            en_idx = st_idx
            while df.index[en_idx] < en_tm and en_idx < len(df) - 1:
                en_idx += 1
            en_idx += 1

            dfs[i] = df[st_idx:en_idx]

        # Drop rows which are not intersection of every dataframes
        idx_int = set(dfs[0].index)
        for df in dfs[1:]:
            idx_int = idx_int.intersection(set(df.index))

        for i in range(len(dfs)):
            drop_idx = list(set(dfs[i].index) - idx_int)
            for idx in drop_idx:
                dfs[i] = dfs[i].drop(idx)

    @staticmethod
    def accumulate(df, interval, verbose=False):
        st_tm = df.index[0] + interval
        st_idx = 0
        while df.index[st_idx] < st_tm:
            st_idx += 1
        st_idx -= 1

        cols = df.columns.values.tolist()
        cols.append('Date')
        res = pd.DataFrame(columns=cols)
        res.set_index('Date', inplace=True)

        total_cnt = len(df) - st_idx
        for i in range(st_idx, len(df)):
            curr_idx = df.index[i]
            chunk = df[(curr_idx - interval < df.index) & (df.index <= curr_idx)]
            new_row = {
                'Open': chunk.iloc[0]['Open'],
                'Adj Open': chunk.iloc[0]['Adj Open'],
                'High': max(chunk['High']),
                'Adj High': max(chunk['Adj High']),
                'Low': min(chunk['Low']),
                'Adj Low': min(chunk['Adj Low']),
                'Close': chunk.iloc[-1]['Close'],
                'Adj Close': chunk.iloc[-1]['Adj Close'],
                'Volume': np.sum(chunk['Volume'])
            }

            res.loc[curr_idx] = new_row

            if (i % 100 == 0 or i == len(df) - 1) and verbose is True:
                curr_cnt = i - st_idx + 1
                progress = round(curr_cnt / total_cnt * 100, 2)
                print('\r{}%     '.format(progress), end='')

        print()
        return res

    def update_acc(self, tickers, intervals=None, verbose=False, force_update=False, update_dt=86400):
        if intervals is None:
            intervals = [
                ['1d', relativedelta(days=1)],
                ['1w', relativedelta(weeks=1)],
                ['1M', relativedelta(months=1)]
            ]

        for idx, ticker in enumerate(tickers):
            cache_file = '{}_acc.cache'.format(ticker)
            cache_path = os.path.join(self.cache_dir, cache_file)

            if os.path.isfile(cache_path) and force_update is False:
                m_time = os.path.getmtime(cache_path)

                # Don't update if data is recently updated
                if (time.time() - m_time) <= update_dt:
                    if verbose is True:
                        print('({}/{}) [{}](Acc) Recently updated'.format(idx + 1, len(tickers), ticker))
                    continue

            data = self.get(ticker, acc=False)
            res = dict()
            for s, delta in intervals:
                if verbose is True:
                    print('({}/{}) [{}](Acc) {}'.format(idx + 1, len(tickers), ticker, s))
                acc_data = self.accumulate(data, delta, verbose=verbose)
                res[s] = acc_data

            # Save data
            if not os.path.exists(self.cache_dir):
                os.mkdir(self.cache_dir)

            fw = open(cache_path, 'wb')
            pickle.dump(data, fw)

            if verbose is True:
                print('({}/{}) [{}](Acc) Data updated'.format(idx + 1, len(tickers), ticker))


'''
pd.set_option('display.max_columns', None)
dm = DataManager()
tickers = ['^IXIC', '^GSPC', '^DJI',
           '^IRX', '^FVX', '^TNX', '^TYX',
           'QQQ', 'QLD', 'TQQQ', 'PSQ', 'QID', 'SQQQ',
           'BRK-B']
dm.update(tickers, force_update=False)
dm.update_acc(tickers, force_update=True, verbose=True)
'''