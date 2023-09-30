import os
import copy

from datamgr.datamgr import DataManager


class Backtest:
    def __init__(self, tickers, sz_acc_list, cache_dir):
        self.tickers = tickers
        self.cache_dir = os.path.abspath(cache_dir)

        self.dm = DataManager(cache_dir)

        # Update data
        print('─────────────── Update Data ───────────────')
        self.dm.update(tickers, force_update=False, verbose=True)
        print()
        print('─────────────── Update Acc Data ───────────────')
        self.dm.acc_update(tickers, sz_acc_list, force_update=False, verbose=True)

        # Get and match date
        self.data = self.dm.get(tickers, acc=True)
        self.dm.match_date(self.data)

    def create_sim(self, sz_base):
        return Backtest.Simulation(self, sz_base)

    class Balance:
        def __init__(self, sim):
            self.sim = sim
            self.cash = 0
            self.stock = dict()

        def add_cash(self, cash_amount):
            self.cash += cash_amount

        def add_stock(self, ticker, qty, price_pos):
            price = self.sim.get_curr_price(ticker, price_pos)
            if self.cash < price:
                return False

            self.cash -= price * qty

            # Add new stock
            if ticker not in self.stock.keys():
                self.stock[ticker] = 0

            self.stock[ticker] += qty
            return True

        def rm_stock(self, ticker, qty, price_pos):
            if ticker not in self.stock.keys():
                return False

            if self.stock[ticker] < qty:
                return False

            price = self.sim.get_curr_price(ticker, price_pos)
            self.cash += price * qty
            self.stock[ticker] -= qty

            if self.stock[ticker] == 0:
                del self.stock[ticker]

            return True

        def get_curr_balance(self, price_pos):
            res = 0
            res += self.cash

            for ticker in list(self.stock.keys()):
                res += self.sim.get_curr_price(ticker, price_pos)

            return res

    class Simulation:
        def __init__(self, parent, sz_base):
            self.parent = parent
            self.data = self.parent.data
            self.sz_base = sz_base
            self.date_range = DataManager.get_datetime_range(self.parent.data)

            self.balance = Backtest.Balance(self)
            self.balance_hist = []

            self.curr_idx = 0

        def get_state(self):
            pass

        def buy(self, ticker, qty, price_pos):
            return self.balance.add_stock(ticker, qty, price_pos)

        def sell(self, ticker, qty, price_pos):
            return self.balance.rm_stock(ticker, qty, price_pos)

        def add_cash(self, cash_amount):
            self.balance.add_cash(cash_amount)

        def get_balance(self):
            return self.balance

        def step(self):
            if self.curr_idx == self.get_data_len() - 1:
                return False

            self.curr_idx += 1
            self.balance_hist.append(copy.deepcopy(self.balance))
            return True

        def set_curr_idx(self, idx):
            if idx >= self.get_data_len():
                return False

            self.curr_idx = idx
            return True

        def get_statistics(self):
            pass

        def get_data_len(self):
            ticker = list(self.data.keys())[0]
            return len(self.data[ticker][self.sz_base].index)

        def get_curr_datetime(self):
            ticker = list(self.data.keys())[0]
            return self.data[ticker][self.sz_base].index[self.curr_idx]

        def get_curr_price(self, ticker, price_pos):
            price = self.data[ticker][self.sz_base][price_pos].iloc[[self.curr_idx]]
            return round(price, 4)


def test():
    tickers = ['^IXIC', 'QQQ', 'QLD', 'TQQQ', 'PSQ', 'QID']
    bt = Backtest(tickers, [1, 5, 30], './datamgr/cache')
    sim = bt.create_sim(sz_base=1)
    print(sim.get_data_len())
    print(sim.get_curr_datetime())


if __name__ == '__main__':
    test()
