from pytest import fixture
from config import Config

import sys
sys.path.append('../.')
from src.connectors.coinbase import CoinbaseClient


def pytest_addoption(parser):
    parser.addoption(
                    '--env',
                    action='store',
                    help='Environment to run tests against, options are: live or sandbox'
    )


@fixture(scope='session')
def env(request):
    return request.config.getoption('--env')


@fixture(scope='session')
def app_config(env):
    cfg = Config(env)
    return cfg


@fixture(scope='function')
def coinbase_client(env):
    if env == 'sandbox':
        coinbase = CoinbaseClient(sandbox=True)
    else:
        coinbase = CoinbaseClient(sandbox=False)

    yield coinbase


@fixture(scope='function')
def btc_id():
    return 'BTC-USD'
