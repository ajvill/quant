import json
import sys
import os
from src.quant_db import quant_db_utils
from pytest import fixture

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.getcwd())

watchlist_members_data = "files/watchlist_members_data.json"
master_watchlist_data = "files/master_watchlist_data.json"
accounts_data = "files/accounts_data.json"

def load_test_data(path):
    with open(path) as data_file:
        data = json.load(data_file)
        return data


@fixture(scope='function')
def quant_tables():
    return quant_db_utils.QUANT_TABLES


@fixture(scope='function')
def quant_indexes():
    return quant_db_utils.QUANT_INDEXES


@fixture(params=load_test_data(watchlist_members_data).values())
def watchlist_members_data(request):
    data = request.param
    return data


@fixture(params=load_test_data(master_watchlist_data).values())
def master_watchlist_data(request):
    data = request.param
    return data

@fixture(params=load_test_data(accounts_data).values())
def accounts_data(request):
    data = request.param
    return data
