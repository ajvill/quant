import json
import sys
import os
from pytest import fixture

os.chdir('exchanges')
sys.path.append(os.getcwd())

btc_limit_order_data = 'files/btc_limit_order_tests.json'
btc_mkt_order_size_data = 'files/btc_market_order_size_tests.json'
btc_mkt_order_funds_data = 'files/btc_market_order_funds_tests.json'
btc_stop_order_data = 'files/btc_stop_loss_order_tests.json'
btc_get_all_fills_data = 'files/btc_get_all_fills_tests.json'
get_all_orders_data = 'files/get_all_orders_tests.json'


def load_test_data(path):
    with open(path) as data_file:
        data = json.load(data_file)
        return data


@fixture(scope='function')
def btc_usd():
    return 'BTC-USD'


@fixture(scope='function')
def btc_acct_id():
    return 'cd3bd0ec-8cd5-470a-a39d-265e8c59d82c'


@fixture(scope='function')
def cb_order_resp_key_list():
    """
    List of keys in the exchange response for testing
    """
    order_resp_key_list = ['id', 'price', 'size', 'product_id', 'side', 'stp', 'stop', 'stop_price',
                           'funds', 'type', 'post_only', 'created_at', 'fill_fees', 'filled_size',
                           'executed_value', 'status', 'settled', 'time_in_force']

    return order_resp_key_list


@fixture(params=load_test_data(btc_limit_order_data).values())
def cb_limit_order_from_fixture(request):
    data = request.param
    return data


@fixture(params=load_test_data(btc_mkt_order_size_data).values())
def cb_mkt_order_size_from_fixture(request):
    data = request.param
    return data


@fixture(params=load_test_data(btc_mkt_order_funds_data).values())
def cb_mkt_order_funds_from_fixture(request):
    data = request.param
    return data


@fixture(params=load_test_data(btc_stop_order_data).values())
def cb_stop_order_from_fixture(request):
    data = request.param
    return data


@fixture(params=load_test_data(btc_get_all_fills_data).values())
def cb_get_all_fills_from_fixture(request):
    data = request.param
    return data


@fixture(params=load_test_data(get_all_orders_data).values())
def cb_get_all_orders_from_fixture(request):
    data = request.param
    return data
