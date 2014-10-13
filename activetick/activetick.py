import pandas as pd
import datetime as dt
import httplib, subprocess

class ActiveTick:
    """ ActiveTick Feed HTTP Server. """

    def __init__(self, ip_address='127.0.0.1', port=5000):
        self.ip_address = ip_address
        self.port = port
        self.http_server = httplib.HTTPConnection(ip_address, port)

    def connect(self):
        self.http_server.connect()

    def transform_symbol(self, symbol, asset_class):
        """
        ActiveTick's symbology
        """
        if asset_class == 'equity':
            symbol = symbol.replace('/', '-')
        elif asset_class == 'index':
            symbol = 'INDEX:' + symbol
        elif asset_class == 'currency':
            symbol = 'CURRENCY:' + symbol
        else:
            symbol = 'OPTION:' + symbol.replace(' ', '-')
        return symbol

    def request_bars(self, symbol, period=0, k=1, start=dt.datetime(2005, 1, 1), end=dt.datetime.now()):
        """
		Request historical bars.

        @param symbol
        @param period 0=intraday minutes, 1=daily, 2=weekly
        @param k 1-60
        @param start a datetime.datetime object specifying the time of the first bar
        @param end a datetime.datetime object specifying the time of the last bar

        @return a pandas DataFrame of asset prices (OHLCV)
        """
        def to_df(bars):
            def format_bar(x):
                x = x.split(',')
                x[0] = dt.datetime.strptime(x[0], '%Y%m%d%H%M%S')
                x[1], x[2], x[3], x[4] = float(x[1]), float(x[2]), float(x[3]), float(x[4])
                x[5] = int(x[5])
                return x
            df = map(lambda x: format_bar(x), bars)
            columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
            df = pd.DataFrame(data=df, columns=columns).set_index('Datetime')
            return df
        begin_time, end_time = start, end
        data = pd.DataFrame()
        while True:
            url = ''.join(['/barData?symbol=%s' % symbol,
                           '&historyType=%s' % period,
                           '&intradayMinutes=%s' % k,
                           '&beginTime=%s' % int(begin_time.strftime('%Y%m%d%H%M%S')),
                           '&endTime=%s' % int(end_time.strftime('%Y%m%d%H%M%S'))])
            self.http_server.request('GET', url)
            response = self.http_server.getresponse()
            bars = response.read().splitlines()
            try:
                # Transform bars into a pandas DataFrame and append it
                data = data.append(to_df(bars=bars))
                # Use the earliest bar's index minus 1 bar as the new end_date
                if period == 0:
                    end_time = data.index.min().to_datetime() - dt.timedelta(minutes=1)
                elif period == 1:
                    end_time = data.index.min().to_datetime() - dt.timedelta(days=1)
                else:
                    end_time = data.index.min().to_datetime() - dt.timedelta(weeks=1)
                begin_time = end_time - dt.timedelta(days=100)
                if begin_time < start:
                    begin_time = start
                if end_time <= start:
                    break
            except ValueError:
                break
        data = data[['Open', 'High', 'Low', 'Close', 'Volume']].sort()
        return data[start:end]

    def request_option_chain(self, symbol):
        """
        Request option chain of a symbol.

        @param symbol option symbol following OSI

        @return a list of options symbols
        """
        url = '/optionChain?symbol=%s' % symbol
        self.http_server.request('GET', url)
        response = self.http_server.getresponse()
        return response.read().splitlines()[:-1]

    def request_trades(self, symbol, start, end):
        """
        Request all trades between two dates.

        @param symbol a string specifying the symbol to be requested
        @param start retrieve data no earlier than this datetime.datetime object
        @param end retrieve data through this datetime.datetime object

        @return a pandas DataFrame of tick data (Datetime, Price, Size, Exchange,
                Cond1, Cond2, Cond3, Cond4)
        """
        if start >= end:
            print 'Stop! Arguments must respect the condition end > start.'
            return
        def to_df(x):
            def format_bars(x):
                x = x.split(',')[1:]
                x[0] = dt.datetime.strptime(str(x[0]), '%Y%m%d%H%M%S%f')
                x[1] = float(x[1])
                x[2], x[4], x[5], x[6], x[7] = int(x[2]), int(x[4]), int(x[5]), int(x[6]), int(x[7])
                x[3] = str(x[3])
                return x
            df = map(lambda x: format_bars(x), bars)
            columns = ['Datetime', 'Price', 'Size', 'Exchange', 'Cond1', 'Cond2', 'Cond3', 'Cond4']
            df = pd.DataFrame(data=df, columns=columns).set_index('Datetime')
            return df
        begin_time, end_time = start, end
        data = pd.DataFrame()
        while True:
            url = ''.join(['/tickData?symbol=%s&trades=1&quotes=0' % symbol,
                           '&beginTime=%s' % begin_time.strftime('%Y%m%d%H%M%S'),
                           '&endTime=%s' % end_time.strftime('%Y%m%d%H%M%S')])
            self.http_server.request('GET', url)
            response = self.http_server.getresponse()
            bars = response.read().splitlines()
            if bars == ['0']:
                break
            try:
                data = data.append(to_df(x=bars))
                begin_time = data.index.max().to_datetime() + dt.timedelta(milliseconds=1)
                if len(bars) < 20000:
                    break
            except ValueError:
                break
        data = data[['Price', 'Size', 'Exchange', 'Cond1', 'Cond2', 'Cond3', 'Cond4']].sort()
        return data

    def request_quotes(self, symbol, start, end):
        """
        Request all quotes between two dates.

        @param symbol a string specifying the symbol to be requested
        @param start retrieve data no earlier than this datetime.datetime object
        @param end retrieve data through this datetime.datetime object

        @return a pandas DataFrame of quotes (BidPrice, AskPrice, BidSize, AskSize,
                BidExchange, AskExchange, Cond)
        """
        if start == end:
            print 'Warning! Start and end date/times can not be identical.'
            return None
        def to_df(x):
            def format_bars(x):
                x = x.split(',')[1:]
                x[0] = dt.datetime.strptime(str(x[0]), '%Y%m%d%H%M%S%f')
                x[1], x[2] = float(x[1]), float(x[2])  # bid, ask
                x[3], x[4], x[7] = int(x[3]), int(x[4]), int(x[7])
                x[5], x[6] = str(x[5]), str(x[6])
                return x
            df = map(lambda x: format_bars(x), bars)
            columns = ['Datetime', 'BidPrice', 'AskPrice', 'BidSize', 'AskSize',
                       'BidExchange', 'AskExchange', 'Cond']
            df = pd.DataFrame(data=df, columns=columns).set_index('Datetime')
            return df
        begin_time, end_time = start, end
        data = pd.DataFrame()
        while True:
            url = ''.join(['/tickData?symbol=%s&trades=0&quotes=1' % symbol,
                           '&beginTime=%s' % begin_time.strftime('%Y%m%d%H%M%S'),
                           '&endTime=%s' % end_time.strftime('%Y%m%d%H%M%S')])
            self.http_server.request('GET', url)
            response = self.http_server.getresponse()
            bars = response.read().splitlines()
            if bars == ['0']:
                break
            try:
                data = data.append(to_df(x=bars))
                begin_time = data.index.max().to_datetime() + dt.timedelta(milliseconds=1)
                if len(bars) < 20000:
                    break
            except ValueError:
                break
        data = data[['BidPrice', 'AskPrice', 'BidSize', 'AskSize',
                     'BidExchange', 'AskExchange', 'Cond']].sort()
        return data
