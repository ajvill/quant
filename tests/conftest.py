from pytest import fixture
from config import Config
from src.connectors.coinbase import CoinbaseClient
import sys
sys.path.append('../.')


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
def btc_usd():
    return 'BTC-USD'


@fixture(scope='function')
def btc_acct_id():
    return 'cd3bd0ec-8cd5-470a-a39d-265e8c59d82c'


@fixture(scope='function')
def cb_limit_order_resp_key_list():
    """
    List of keys in the exchange response for testing
    """
    order_resp_key_list = ['id', 'price', 'size', 'stp', 'type', 'time_in_force',
                           'post_only', 'created_at', 'fill_fees', 'filled_size',
                           'executed_value', 'status', 'settled'
                           ]
    return order_resp_key_list


@fixture(scope='function')
def cb_market_order_resp_key_list():
    """
    List of keys in the exchange response for testing
    """
    order_resp_key_list = ['id', 'size', 'product_id', 'side', 'stp', 'funds', 'type', 'post_only', 'created_at',
                           'fill_fees', 'filled_size', 'executed_value', 'status', 'settled'
                           ]

    return order_resp_key_list
