import os

from datetime import timedelta

from datamgr.datamgr import DataManager
from datamgr.interval_conf import interval_deltas

class BackTest:
    def __init__(self, tickers, intervals=None, force_update=False, root_dir='.'):
        self.root = root_dir

        if intervals is None:
            intervals = ['1d', '1w', '1M']

        self.dm = DataManager(root_dir=os.path.join(self.root, 'datamgr'))
        self.dm.update(tickers, force_update=force_update, verbose=True)
        print()
        self.dm.update_acc(tickers, intervals=intervals, force_update=force_update, verbose=True)

        self.data = dict()
        for ticker in tickers:
            data_buf = self.dm.get(ticker, acc=True)
            self.data[ticker] = data_buf
        self.__match_data()

    def __match_data(self):
        datas = []
        for ticker in list(self.data.keys()):
            for interval in list(self.data[ticker].keys()):
                datas.append(self.data[ticker][interval])
        self.dm.match(datas)

        idx = 0
        for ticker in list(self.data.keys()):
            for interval in list(self.data[ticker].keys()):
                self.data[ticker][interval] = datas[idx]
                idx += 1

    def get_tickers(self):
        return list(self.data.keys())

    def get_data_datetime_range(self):
        if len(self.data) == 0:
            return None, None

        sample_ticker = self.get_tickers()[0]
        sample_interval = list(self.data[sample_ticker].keys())[0]
        sample_data = self.data[sample_ticker][sample_interval]

        return sample_data.index[0], sample_data.index[-1]

    def new_sim(self, st_tm=None, en_tm=None, sz_step=timedelta(minutes=1)):
        datetime_rng = self.get_data_datetime_range()

        if st_tm is None:
            st_tm = datetime_rng[0]

        if en_tm is None:
            en_tm = datetime_rng[1]

        sim = self.Simulator(self, st_tm, en_tm, sz_step)
        return sim

    class Simulator:
        def __init__(self, parent, st_tm, en_tm, sz_step):
            self.parent = parent
            self.data = parent.data
            self.curr_tm = st_tm
            self.en_tm = en_tm
            self.sz_step = sz_step

        def get_tickers(self):
            return list(self.parent.data.keys())

        def get_data_datetime_range(self):
            return self.parent.get_data_datetime_range()

        def step(self, sz_step=None):
            if sz_step is not None:
                sz_step = self.sz_step

            if self.curr_tm + sz_step > self.en_tm:
                return False

            self.curr_tm += sz_step
            return True

        def get_status(self):
            pass


def test():
    tickers = ['^IXIC', 'QQQ', 'QLD', 'TQQQ', 'PSQ', 'QID']

    bt = BackTest(tickers, force_update=False)
    print()
    datetime_rng = bt.get_data_datetime_range()
    print('Datetime Range : {}, {}'.format(datetime_rng[0], datetime_rng[1]))
    print('Tickers :', bt.get_tickers())

    sim1 = bt.new_sim(st_tm=datetime_rng[0], en_tm=datetime_rng[1],
                      sz_step=interval_deltas['1m'])


if __name__ == '__main__':
    test()
