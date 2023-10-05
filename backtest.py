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

    def create_sim(self, sz_base, buy_fee, sell_fee):
        return Backtest.Simulation(self, sz_base, buy_fee, sell_fee)

    class Balance:
        def __init__(self, sim):
            self.sim = sim
            self.cash = 0
            self.stock = dict()

        def get_curr_value(self, price_pos):
            res = 0
            res += self.cash

            for ticker in list(self.stock.keys()):
                res += self.sim.get_curr_price(ticker, price_pos)

            return res

        def add_stock_qty(self, ticker, qty):
            if ticker not in self.stock.keys():
                self.stock[ticker] = 0

            if self.stock[ticker] + qty < 0:
                return False

            self.stock[ticker] += qty
            return True

    class Simulation:
        def __init__(self, parent, sz_base, buy_fee, sell_fee):
            self.parent = parent
            self.data = self.parent.data
            self.sz_base = sz_base
            self.date_range = DataManager.get_datetime_range(self.parent.data)

            self.buy_fee = buy_fee
            self.sell_fee = sell_fee
            self.paid_fee = 0

            self.balance = Backtest.Balance(self)
            self.balance_hist = []

            self.curr_idx = 0

        # Index Controls
        # ========================================================================
        def step(self):
            if self.curr_idx == self.get_data_len() - 1:
                return False

            self.curr_idx += 1
            self.balance_hist.append({
                'obj': self.get_balance(),
                'value': self.balance.get_curr_value('Adj Close')}
            )
            return True

        def set_curr_idx(self, idx):
            if idx >= self.get_data_len():
                return False

            self.curr_idx = idx
            return True

        # Get Values
        # ==========================================================================
        def get_data_len(self):
            ticker = list(self.data.keys())[0]
            return len(self.data[ticker][self.sz_base].index)

        def get_curr_datetime(self):
            ticker = list(self.data.keys())[0]
            return self.data[ticker][self.sz_base].index[self.curr_idx]

        def get_curr_price(self, ticker, price_pos):
            price = self.data[ticker][self.sz_base][price_pos].iloc[[self.curr_idx]]
            return round(price)

        def get_balance(self):
            return copy.deepcopy(self.balance)

        # Statistics
        # ==========================================================================
        def get_state(self, tickers, counts):
            res = dict()
            for ticker in tickers:
                res[ticker] = dict()

            for sz_acc, cnt in counts.items():
                req_min_idx = sz_acc * (cnt - 1)
                if self.curr_idx < req_min_idx:
                    return None

                for ticker in tickers:
                    idx_from = self.curr_idx - sz_acc * (cnt - 1)
                    res[ticker][sz_acc] = self.data[ticker][sz_acc].iloc[idx_from:self.curr_idx+1:sz_acc]

            return res

        def get_history(self):
            res = {
                'balance_history': copy.deepcopy(self.balance_hist),
                'paid_fee': self.paid_fee
            }

            return res

        # Actions
        # ==========================================================================
        def add_cash(self, cash_amount):
            self.balance.cash += cash_amount

        def buy(self, ticker, qty, price_pos):
            ticker_price = self.get_curr_price(ticker, price_pos)
            fee = ticker_price * qty * self.buy_fee
            cash_need = ticker_price * qty + fee
            if self.balance.cash < cash_need:
                return False

            self.balance.cash -= cash_need
            self.paid_fee += fee
            self.balance.add_stock_qty(ticker, qty)

            return True

        def sell(self, ticker, qty, price_pos):
            res = self.balance.add_stock_qty(ticker, -qty)
            if res is False:
                return False

            ticker_price = self.get_curr_price(ticker, price_pos)
            fee = ticker_price * qty * self.sell_fee
            cash_income = ticker_price * qty - fee
            self.paid_fee += fee
            self.balance.cash += cash_income

            return True


def test():
    tickers = ['^IXIC', 'QQQ', 'QLD', 'TQQQ', 'PSQ', 'QID']
    bt = Backtest(tickers, [1, 5, 20], './datamgr/cache')
    sim = bt.create_sim(sz_base=1, buy_fee=0.003, sell_fee=0.0)
    print(sim.get_data_len())
    sim.set_curr_idx(120)
    print(sim.get_curr_datetime())
    print(sim.get_state(['^IXIC', 'QLD'], {1: 5, 5: 4, 20: 6}))


if __name__ == '__main__':
    test()
