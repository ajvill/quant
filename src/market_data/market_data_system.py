from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import os
import re
import logging

logger = logging.getLogger()


class MarketDataSystem:
    def __init__(self):
        self._financial_data_policies = {
            'alpha_vantage': AlphaVantagePolicy,
            'yfinance': YFinancePolicy
        }

    def get_policy(self, policy_id):
        policy_type = self._financial_data_policies.get(policy_id)
        if not policy_id:
            raise ValueError(policy_id)

        return policy_type


class MarketDataPolicy:
    def __init__(self):
        self.tv_watchlist_data = self.parse_tv_watchlists()
        self.tv_df = pd.DataFrame(self.tv_watchlist_data)

    @property
    def tv_crypto_df(self):
        df = self.tv_df
        df = df[df['exchange'] == 'COINBASE']
        df = df.drop([df[df['ticker'].str.match('\w+(/)\w+')].index[0]])
        df.index = range(len(df))

        return df

    @property
    def tv_crypto_list(self):
        df = self.tv_crypto_df
        crypto_list = df['ticker'].unique().tolist()

        return crypto_list

    @property
    def tv_stock_df(self):
        drop_exchange_list = ['OANDA', 'TVC', 'NYMEX',
                              'FOREXCOM', 'NSE', 'CME_MINI', 'COMEX', 'CBOE', 'COINBASE', 'CME',
                              'FX', 'FX_IDC', 'ASX', 'DJ', 'SP', 'FWB', 'EURONEXT', 'TSX',
                              'MOEX', 'JSE', 'CBOT_MINI', 'CBOT', 'BITMEX', 'LSIN', 'ICEUS',
                              'IDX', 'LSE', 'NEWCONNECT', 'BSE', 'SIX', 'INDEX', 'TSE', 'SSE',
                              'KRX', 'USI', 'BNC', 'CAPITALCOM']

        df = self.tv_df
        df = df[~df.exchange.isin(drop_exchange_list)]
        df.index = range(len(df))

        return df

    @property
    def tv_stock_list(self):
        df = self.tv_stock_df
        stock_list = df['ticker'].unique().tolist()

        return stock_list

    @property
    def tv_commodity_tickers(self):
        commodity_tickers = []

        return None

    @property
    def tv_futures_tickers(self):

        return None

    @classmethod
    def parse_tv_watchlists(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        os.chdir('../../accounts/tv_watchlists')
        files = os.listdir()
        wl_list = []
        data = {}

        file_count = 0
        ticker_count = 0
        for file in files:
            f = open(file, 'r')
            line = f.readline()
            line_list = line.split(',')
            wl_name_test = file.split('.')[0]
            if re.search(r'(IWM)', wl_name_test):
                watchlist = re.search(r'(IWM)', wl_name_test).group()
            else:
                watchlist = wl_name_test
            for elem in line_list:
                try:
                    exchange = elem.split(':')[0]
                    ticker = elem.split(':')[1]
                    data = {'ticker': ticker, 'exchange': exchange, 'watchlist': watchlist}
                    wl_list.append(data)
                    ticker_count += 1
                except Exception as error:
                    logger.error("failed on file: {} and elem {}".format(file, elem))
            file_count += 1
            f.close()
        logger.info("total_files = {}, ticker_count = {}, file = {}".format(file_count, ticker_count, file))

        return wl_list


class AlphaVantagePolicy(MarketDataPolicy):
    def __init__(self, api_setting=75):
        super().__init__()
        self.api_setting = api_setting
        self.ts = TimeSeries(key=self.get_api_key(), output_format='pandas')

    @staticmethod
    def get_api_key():
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        with open('../../accounts/alpha_vantage.txt', 'r') as api_file:
            key = api_file.readline()
        api_file.close()

        return key


class YFinancePolicy(MarketDataPolicy):
    def __init__(self):
        super().__init__()


class CoinbasePolicy(MarketDataPolicy):
    def __init__(self):
        super().__init__()


class IBPolicy(MarketDataPolicy):
    def __init__(self):
        super().__init__()
