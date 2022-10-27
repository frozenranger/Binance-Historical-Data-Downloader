import ccxt
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

class Download(object):

    def __init__(self, days, offset):
        self.exchange = ccxt.binance()

        _days = days
        self.msec = 1000
        self.candle_time = 1 # candle time in mins
        self.minute = self.candle_time * 60 * self.msec
        #print("working")

        #print(datetime.fromtimestamp(self.from_timestamp/1000))
        #this time delta is needed because python is too slow and ends up collecting information for an incomplete candle
        now = datetime.now().replace(second=0, microsecond=0) - timedelta(minutes=1)

        if offset:
            self.td_offset = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
            from_datetime = self.td_offset - timedelta(days=_days)
        else:
            from_datetime = now - timedelta(days=_days+(1/(24*60)))

        self.from_timestamp = int(datetime.timestamp(from_datetime)) * self.msec  # self.exchange.parse8601(from_datetime)

        self.now_timestamp = datetime.timestamp(now) * 1000
        self.pd_open = []
        self.pd_high = []
        self.pd_low = []
        self.pd_close = []
        self.pd_volume = []
        self.dates = []

    def get_data(self, ticker):
        while self.from_timestamp < self.now_timestamp:
            data_list = self.exchange.fetch_ohlcv(ticker, '1m', self.from_timestamp)
            if data_list:
                for x in data_list:
                    date = datetime.fromtimestamp(x[0] / self.msec)
                    # print(date)
                    self.dates.append(date)

                    # datetime.strptime(date, '%Y-%m-%d %H:%M:%S'))  # note difference between strp and strf
                    self.pd_open.append(x[1])
                    self.pd_high.append(x[2])
                    self.pd_low.append(x[3])
                    self.pd_close.append(x[4])
                    self.pd_volume.append(x[5])

                    # pd_color.append('green' if x[4] > x[1] else 'red')

                    #if x == data_list[-1]:
                    if self.from_timestamp == self.now_timestamp:
                        break
                    else: 
                        self.from_timestamp += self.minute
                        #self.from_timestamp += len(data_list) * self.minute

            else:
                return None



        # print("from time", datetime.fromtimestamp(self.from_timestamp / self.msec));
        # print("now", datetime.fromtimestamp(self.now_timestamp / self.msec));
        date_position_holder = datetime.fromtimestamp(data_list[-1][0] / self.msec)
        data_list = np.array(self.exchange.fetch_ohlcv(ticker, '1m'))
        location = np.argwhere(data_list == datetime.timestamp(date_position_holder) * self.msec)  # tight way to find index of a value in a 2d array
            # print(datetime.fromtimestamp(data_list[location[0][0]][0]/1000) == dates[-1])
        try:
            for x in data_list[location[0][0] + 1:]:
                date = datetime.fromtimestamp(x[0] / self.msec)
                # print(date)
                self.dates.append(date)
                self.pd_open.append(x[1])
                self.pd_high.append(x[2])
                self.pd_low.append(x[3])
                self.pd_close.append(x[4])
                self.pd_volume.append(x[5])
        except Exception as e:
            print(e, ticker)
            # pd_color.append('green' if x[4] > x[1] else 'red')


        # df = pd.DataFrame(np.array([self.dates, self.pd_open, self.pd_high, self.pd_low, self.pd_close, self.pd_volume]).T,
        #                   columns=['Date', 'Open', 'High',
        #                            'Low', 'Close', 'Volume'])


        df = pd.DataFrame(np.array([pd.to_datetime(self.dates, utc=True), self.pd_open, self.pd_high, self.pd_low, self.pd_close, self.pd_volume]).T,
                    columns=['Date', 'Open', 'High',
                            'Low', 'Close', 'Volume'])


        df.set_index('Date', inplace=True)
        df.index = df.index.floor('S').tz_localize(None)  # nice clean up, not needed for u64 timestamps
        #df.index.name = None


        #ticker = str(ticker).replace('/', '')
        #df.to_pickle(ticker + '.pkl')

        # set this location to whatever you want
        df.to_csv ("C:\\Users\\peter\\rustprojects\\rust_test1\\src\\" + ticker.lower() + ".csv")
        return df



    def read_data(self, ticker):
        df = pd.read_pickle(ticker + '.pkl')
        return df


if __name__ == "__main__":
    #True: start from 12am
    #False: start from current time - 24hrs
    now = datetime.now().replace(microsecond=0)
    while now.second != 0:
        print(f"Waiting until next minute to sync {now}\r", end="")
        now = datetime.now().replace(microsecond=0)

    print()
    #download object needs number of days in the past and whether or not to start from midnight
    d = Download(90, True) #30 is similar to trading veiw
    d.get_data("BTCUSDT") # must be upper case






