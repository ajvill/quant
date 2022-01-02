import pandas as pd

from src.market_data.market_data_system import *
from pytest import mark
import logging

logger = logging.getLogger()


class MarketDataTests:

    @mark.md_policy
    def test_mdp_objects(self):
        mdp = MarketDataPolicy()
        avp = AlphaVantagePolicy()
        yfp = YFinancePolicy()

        assert isinstance(mdp, MarketDataPolicy)
        assert isinstance(avp, AlphaVantagePolicy)
        assert isinstance(yfp, YFinancePolicy)

    @mark.md_policy
    def test_mdp_crypto_tickers(self):
        mdp = MarketDataPolicy()

        crypto_df = mdp.tv_crypto_df
        assert isinstance(crypto_df, pd.DataFrame)

        crypto_tickers = mdp.tv_crypto_list
        assert isinstance(crypto_tickers, list)

    @mark.md_policy
    def test_mdp_stock_tickers(self):
        mdp = MarketDataPolicy()

        stock_df = mdp.tv_stock_df
        assert isinstance(stock_df, pd.DataFrame)

        stock_list = mdp.tv_stock_list
        assert isinstance(stock_list, list)

    @mark.md_policy
    def test_avp_timeseries_object(self):
        avp = AlphaVantagePolicy()

        assert isinstance(avp.ts, TimeSeries)
