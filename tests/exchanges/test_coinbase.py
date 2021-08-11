import time

from pytest import mark
import pandas as pd


@mark.coinbase
class CoinbaseTests:

    @mark.exchanges
    def test_coinbase_connection(self, coinbase_client):
        coinbase = coinbase_client
        if coinbase is not None:
            assert True
        else:
            assert False

    @mark.cb_products
    def test_get_products(self, coinbase_client):
        coinbase = coinbase_client
        products = coinbase.get_products()

        if products is not None:
            # products should be a list of dicts.  we will check the first row and 'id' key existence
            for row in products:
                if type(row['id']) == str:
                    assert True
                    break
                else:
                    assert False
                    break
        else:
            assert False

    @mark.cb_products
    def test_get_single_product(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        product = coinbase.get_single_product(btc_id)

        if product is not None:
            if type(product['id']) == str:
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_contracts(self, coinbase_client):
        coinbase = coinbase_client
        contracts = coinbase.get_contracts()

        if contracts is not None:
            if len(contracts.keys()) > 0:
                print('\ncontract keys = {}'.format(contracts.keys()))
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level1(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        params = {
            'level': '1'
        }
        ob_data = coinbase.get_product_ob(btc_id, params)

        if ob_data is not None:
            if len(ob_data['bids']) > 0:
                #print('\nlevel 1 ob_data: {}'.format(ob_data))
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level2(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        params = {
            'level': '2'
        }
        ob_data = coinbase.get_product_ob(btc_id, params)

        if ob_data is not None:
            if len(ob_data['bids']) == 50:
                #print('\nlevel 2 ob_data: {}'.format(ob_data))
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level3(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        params = {
            'level': '3'
        }
        ob_data = coinbase.get_product_ob(btc_id, params)

        if ob_data is not None:
            if len(ob_data['bids']) > 50:
                #print('\nlevel 3 ob_data: {}'.format(ob_data))
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ticker(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        ticker_data = coinbase.get_product_ticker(btc_id)

        # verify what is returned back is has valid data verify by checking price must be greater than zero
        if ticker_data is not None:
            if float(ticker_data['price']) > 0:
                assert True
        else:
            assert False

    @mark.cb_products
    def test_get_trades(self, coinbase_client, btc_id):
        coinbase = coinbase_client
        trade_data = coinbase.get_trades(btc_id)

        trade_data_len = False
        trade_data_keys = False

        if trade_data is not None:
            if len(trade_data) > 0:
                trade_data_len = True
            if len(trade_data[0].keys()) == 5:
                trade_data_keys = True
            if trade_data_len and trade_data_keys:
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_historic_rates(self, coinbase_client, btc_id):
        coinbase = coinbase_client

        # one min, five min, fifteen min, one hour, six hour, one day
        granularity = (60, 300, 900, 3600, 21600, 86400)

        params = {
            'start': '',
            'end': '',
            'granularity': granularity
        }

        interval_pass_counter = 0
        print('\n')
        for i, interval in enumerate(granularity):
            historic_rates = coinbase.get_historic_rates(btc_id, interval)
            interval_pass_counter += 1
            print('interval: {} completed'.format(interval))

        if interval_pass_counter == 6:
            assert True
        else:
            assert False

    @mark.cb_products
    def test_get_24hr_stats(self, coinbase_client, btc_id):
        coinbase = coinbase_client

        stats = coinbase.get_24hr_stats(btc_id)

    @mark.cb_products
    def test_get_currencies(self, coinbase_client):
        coinbase = coinbase_client

        stats = coinbase.get_currencies()

        if len(stats) > 0:
            assert True

    @mark.cb_products
    def test_get_api_time(self, coinbase_client):
        coinbase = coinbase_client

        stats = coinbase.get_api_time()

        if len(stats) == 2:
            assert True

    @mark.cb_products
    def test_list_accounts(self, coinbase_client):
        """
        Calls for a list of accounts from the exchange.  Then check the account keys fields of each account.
        Fail test if field does not match

        :param coinbase_client:
        """
        coinbase = coinbase_client
        accounts_list = coinbase.list_accounts()
        account_key_list = ['id', 'currency', 'balance', 'available', 'hold', 'profile_id', 'trading_enabled']

        for account in accounts_list:
            key_test = [x in account_key_list for x in account.keys()]

            if False in key_test:
                assert False
            else:
                assert True

    @mark.cb_products
    def test_get_an_account(self, coinbase_client):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        account_key_list = ['id', 'balance', 'hold', 'available', 'currency']

        accounts_list = coinbase.list_accounts()
        for acct_dict in accounts_list:
            account = coinbase.get_an_account(acct_dict['id'])
            key_test = [x in account_key_list for x in account.keys()]

            if False in key_test:
                assert False
            else:
                assert True

    @mark.test1
    @mark.cb_products
    def test_get_account_history(self, coinbase_client, btc_id):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        data = {
            'id': '',
            'amount': '',
            'balance': '',
            'created_at': '',
            'type': '',
            'details': {'order_id': '',
                        'product_id': '',
                        'trade_id': ''
                        }
        }

        account_history = coinbase.get_account_history(btc_id)
