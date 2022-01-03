import logging
import sys
sys.path.append('../.')

from pytest import fixture
from config import Config
from src.connectors.coinbase import CoinbaseClient
from src.quant_db.quant_db_utils import QuantDB

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption(
                    '--env',
                    action='store',
                    help='Environment to run tests against, options are: sandbox, live, db'
    )


@fixture(scope='class')
def env(request):
    return request.config.getoption('--env')


@fixture(scope='session')
def app_config(env):
    cfg = Config(env)
    return cfg


@fixture(scope='class')
def coinbase_client(env):
    if env == 'sandbox':
        coinbase = CoinbaseClient(sandbox=True)
    else:
        coinbase = CoinbaseClient(sandbox=False)

    yield coinbase


@fixture(scope='class')
def quant_db_conn(env):
    if env == 'db':
        quantdb = QuantDB()
        conn = quantdb.conn
    elif env == 'db_test':
        quantdb = QuantDB(db_test=True)
        conn = quantdb.conn

    yield conn
