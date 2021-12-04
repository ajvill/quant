import logging
import sys
sys.path.append('../.')

from pytest import fixture
from config import Config
from src.connectors.coinbase import CoinbaseClient
from db_work.quant_db.quant_db_utils import *

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
def quant_db(env):
    if env == 'db':
        conn = get_conn()

    yield conn
