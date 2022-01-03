from src.trader.trader import Trader
from pytest import mark
import logging

logger = logging.getLogger()


class TraderTests:

    @mark.trader
    def test_trader(self):
        trader = Trader()
        assert True
