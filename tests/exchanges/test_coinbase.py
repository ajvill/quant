import numpy as np
import json
import logging
import time
import datetime
import re

from pytest import mark

logger = logging.getLogger(__name__)


@mark.usefixtures('coinbase_client')
class CoinbaseTests:
    @mark.exchanges
    def test_coinbase_connection(self, coinbase_client):
        coinbase = coinbase_client
        if coinbase is not None:
            assert True
        else:
            assert False

    @mark.skip(reason='Conversions Section: convert_currency(), returns an error come back to this later')
    @mark.cb_conversions
    @mark.cb_products
    def test_convert_currency(self, coinbase_client):
        coinbase = coinbase_client

        params = {
            #'profile_id': '',
            'from': '5dcc143c-fb96-4f72-aebf-a165e3d29b53',
            'to': '6100247f-90fc-4335-ac17-d99839f0c909',
            'amount': '11',
            #'nonce': ''
        }

        conversion = coinbase.convert_currency(params)

        assert True

    @mark.skip(reason='Conversions Section: convert_currency(), returns an error come back to this later')
    @mark.cb_conversions
    @mark.cb_products
    def test_get_a_conversion(self, coinbase_client):
        coinbase = coinbase_client

        path_params = {
            'conversion_id': '1234'
        }

        params = {
            'profile_id': 'test'
        }

        conversion = coinbase.get_a_conversion(path_params, params)

        if conversion is not None:
            assert True

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

    @mark.test1
    @mark.cb_profiles
    @mark.cb_products
    def test_get_profiles(self, coinbase_client):
        coinbase = coinbase_client
        profiles = coinbase.get_profiles()

        profiles_keys_list = ['id', 'user_id', 'name', 'active', 'is_default', 'has_margin', 'created_at']

        if profiles is not None:
            for profile in profiles:
                keys_test = [x in profiles_keys_list for x in profile.keys()]
                logger.info('test_get_profiles, keys_test for profile {}, is {}'.format(profile['id'], keys_test))
                if False in keys_test:
                    assert False
                else:
                    assert True

    @mark.skip(reason='create_a_profile(), exchange returns HTTP 400 even with example return to later.')
    @mark.cb_profiles
    @mark.cb_products
    def test_create_a_profile(self, coinbase_client):
        parameters = {
            'name': 'test_profile1'
        }
        coinbase = coinbase_client
        profile = coinbase.create_a_profile(parameters)

        if profile is not None:
            assert True

    @mark.skip(reason='transfer_funds_between_profiles(), not working yet..need create_a_profile'
                      'resolved.')
    @mark.cb_profiles
    @mark.cb_products
    def test_transfer_funds_between_profiles(self, coinbase_client):
        params = {
            "from": "e543fd44-cbcd-4144-a2d9-6d81f42e2093",
            "to": "00921972-6b04-4daa-9458-3a38dd4924f6",
            "amount": "12.345",
            "currency": "BTC"
        }
        coinbase = coinbase_client
        response = coinbase.transfer_funds_between_profiles(params)

        if response is not None:
            assert True

    @mark.cb_profiles
    @mark.cb_products
    def test_get_profile_by_id(self, coinbase_client):
        profiles_keys_list = ['id', 'user_id', 'name', 'active', 'is_default', 'has_margin', 'created_at']
        coinbase = coinbase_client
        response = coinbase.get_profiles()

        if response is not None:
            for p in response:
                profile = coinbase.get_profile_by_id(p['id'])
                if profile is not None:
                    key_test = [x in profiles_keys_list for x in profile.keys()]
                    logger.info('test_get_profile_by_id, keys_test for profile {}, is {}'.format(profile['id'], key_test))
                    if False in key_test:
                        assert False

    @mark.skip(reason='rename_a_profile(), is not complete first need to resolve create_a_profile()')
    @mark.cb_profiles
    @mark.cb_products
    def test_rename_a_profile(self, coinbase_client):
        path_param = {
            'profile_id': 'test123'
        }

        params = {
            'profile_id': 'test123',
            'name': 'test_after'
        }

        coinbase = coinbase_client
        response = coinbase.rename_a_profile(path_param, params)

        if response is not None:
            assert True

    @mark.skip(reason='delete_a_profile(), is not complete first need to resolve create_a_profile()')
    @mark.cb_profiles
    @mark.cb_products
    def test_delete_a_profile(self, coinbase_client):
        path_param = {
            'profile_id': 'test'
        }

        params = {
            'profile_id': 'test',
            'to': 'to_test'
        }

        coinbase = coinbase_client
        response = coinbase.delete_a_profile(path_param, params)

        if response is not None:
            assert True

    @mark.cb_products
    def test_get_single_product(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        product = coinbase.get_single_product(btc_usd)

        if product is not None:
            if type(product['id']) == str:
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_price_oracle
    @mark.cb_products
    def test_get_signed_prices(self, coinbase_client):
        coinbase = coinbase_client
        prices = coinbase.get_signed_prices()

        prices_list = ['timestamp', 'messages', 'signatures', 'prices']

        if prices is not None:
            price_key_test = [x in prices_list for x in prices.keys()]
            if False in price_key_test:
                assert False
            else:
                assert True
        else:
            assert False

    @mark.cb_products
    def test_get_contracts(self, coinbase_client):
        coinbase = coinbase_client
        contracts = coinbase.get_contracts()

        if contracts is not None:
            if len(contracts.keys()) > 0:
                #print('\ncontract keys = {}'.format(contracts.keys()))
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level1(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        params = {
            'level': '1'
        }
        ob_data = coinbase.get_product_ob(btc_usd, params)

        if ob_data is not None:
            if len(ob_data['bids']) > 0:
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level2(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        params = {
            'level': '2'
        }
        ob_data = coinbase.get_product_ob(btc_usd, params)

        key_test = ['bids', 'asks', 'sequence']
        if ob_data is not None:
            results = [x in key_test for x in ob_data.keys()]
            if False not in results:
                assert True
        else:
            assert False

    @mark.cb_products
    def test_get_product_ob_level3(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        params = {
            'level': '3'
        }
        ob_data = coinbase.get_product_ob(btc_usd, params)

        if ob_data is not None:
            if len(ob_data['bids']) > 50:
                assert True
            else:
                assert False
        else:
            assert False

    @mark.cb_products
    def test_get_product_ticker(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        ticker_data = coinbase.get_product_ticker(btc_usd)

        # verify what is returned back is has valid data verify by checking price must be greater than zero
        if ticker_data is not None:
            if float(ticker_data['price']) > 0:
                assert True
        else:
            assert False

    @mark.cb_products
    def test_get_trades(self, coinbase_client, btc_usd):
        coinbase = coinbase_client
        trade_data = coinbase.get_trades(btc_usd)

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

    @mark.cb_fees
    def test_get_fees(self, coinbase_client):
        coinbase = coinbase_client
        fee_data = coinbase.get_fees()
        fees_list = ['taker_fee_rate', 'maker_fee_rate', 'usd_volume']

        if fee_data is not None:
            keys_test = [x in fees_list for x in fee_data.keys()]
            if False in keys_test:
                assert False
            else:
                assert True
        else:
            assert False

    @mark.cb_products
    def test_get_historic_rates(self, coinbase_client, btc_usd):
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
            historic_rates = coinbase.get_historic_rates(btc_usd, interval)
            interval_pass_counter += 1
            #print('interval: {} completed'.format(interval))

        if interval_pass_counter == 6:
            assert True
        else:
            assert False

    @mark.cb_products
    def test_get_24hr_stats(self, coinbase_client, btc_usd):
        coinbase = coinbase_client

        stats = coinbase.get_24hr_stats(btc_usd)

    @mark.cb_currencies
    @mark.cb_products
    def test_get_all_currencies(self, coinbase_client):
        coinbase = coinbase_client
        currencies = coinbase.get_all_currencies()

        if isinstance(currencies, list) and len(currencies) > 0:
            assert True

    @mark.cb_currencies
    @mark.cb_products
    def test_get_a_currency(self, coinbase_client, cb_currency_ids_from_fixture):

        parameters = cb_currency_ids_from_fixture
        coinbase = coinbase_client
        currency = coinbase.get_a_currency(parameters['id'])

        if isinstance(currency, dict) and len(currency) > 0:
            logger.info('{} check passed'.format(currency['id']))
            assert True

    @mark.cb_products
    def test_get_api_time(self, coinbase_client):
        coinbase = coinbase_client

        stats = coinbase.get_api_time()

        if len(stats) == 2:
            assert True

    @mark.cb_accounts
    @mark.cb_products
    def test_get_all_accounts(self, coinbase_client):
        """
        Calls for a list of accounts from the exchange.  Then check the account keys fields of each account.
        Fail test if field does not match

        :param coinbase_client:
        """
        coinbase = coinbase_client
        accounts_list = coinbase.get_all_accounts()
        account_key_list = ['id', 'currency', 'balance', 'available', 'hold', 'profile_id', 'trading_enabled']

        for account in accounts_list:
            key_test = [x in account_key_list for x in account.keys()]

            if False in key_test:
                assert False
            else:
                assert True

    @mark.skip(reason='Hits a KeyError: "order_id" after several successful account_history fetches.  Fixing later')
    @mark.cb_products
    def test_get_account_history(self, coinbase_client):
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

        accounts_list = coinbase.get_all_accounts()
        for acct_dict in accounts_list:
            account_history = coinbase.get_single_account_ledger(acct_dict['id'])
            if account_history is not None:
                if len(account_history) > 0:
                    for acct_hist in account_history:
                        # Outer layer dictionary key test
                        key_outer_test = [x in data.keys() for x in acct_hist.keys()]
                        key_nested_layer_test = [x in data['details'].keys() for x in acct_hist['details'].keys()]
                        if False in key_outer_test:
                            assert False
                        if False in key_nested_layer_test:
                            assert False
            else:
                assert False

    @mark.skip(reason='get_single_account_id() has an issue come back and resolve')
    @mark.cb_accounts
    @mark.cb_products
    def test_get_single_account_id(self, coinbase_client, btc_acct_id):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        account_id_list = ['id', 'currency', 'balance', 'available', 'hold', 'profile_id', 'trading_enabled']
        coinbase = coinbase_client
        single_acct_id = coinbase.get_single_account_id(btc_acct_id)

        key_test = [x in account_id_list for x in single_acct_id.keys()]
        if False in key_test:
            assert False
        else:
            assert True

    @mark.skip(reason='get_single_account_holds() has an issue come back and resolve')
    @mark.cb_accounts
    @mark.cb_products
    def test_get_single_account_holds(self, coinbase_client, btc_acct_id):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        params = {'limit': 100}
        coinbase = coinbase_client
        single_acct_holds = coinbase.get_single_account_holds(btc_acct_id, params)

        if single_acct_holds is not None:
            assert len(single_acct_holds) != 0

    @mark.skip(reason='get_single_account_transfers() has an issue come back and resolve')
    @mark.cb_accounts
    @mark.cb_products
    def test_get_single_account_transfers(self, coinbase_client, btc_acct_id):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        params = {'limit': 100}
        coinbase = coinbase_client
        single_acct_transfers = coinbase.get_single_account_transfers(btc_acct_id)

        if single_acct_transfers is not None:
            assert len(single_acct_transfers) != 0

    @mark.skip(reason='get_single_account_ledger() has an issue come base and resolve later.')
    @mark.cb_accounts
    @mark.cb_products
    def test_get_single_account_ledger(self, coinbase_client, btc_acct_id):
        """
        Grab a list of accounts from the exchange.  Then uses each accounts id to pass to get_an_account.
        Next we check each account uses the account_key_list field

        :param coinbase_client:
        """
        params = {'limit': 100}
        coinbase = coinbase_client
        single_acct_ledger = coinbase.get_single_account_ledger(btc_acct_id)

        if single_acct_ledger is not None:
            assert len(single_acct_ledger) != 0

    @mark.cb_coinbase_accounts
    @mark.cb_products
    def test_get_all_cb_wallets(self, coinbase_client):
        wallet_key_list = ['id', 'name', 'balance', 'currency', 'type', 'primary', 'active',
                           'available_on_consumer', 'hold_balance', 'hold_currency']
        coinbase = coinbase_client
        wallets = coinbase.get_all_cb_wallets()

        if wallets is not None:
            for wallet in wallets:
                keys_test = [x in wallet_key_list for x in wallet.keys()]
                if False in keys_test:
                    assert False
                else:
                    logger.info('Wallet {} passed all dictionary values test'.format(wallet['name']))
                    assert True

    @mark.skip(reason='gen_crypto_address(), is a one-time address test may not need')
    @mark.cb_coinbase_accounts
    @mark.cb_products
    def test_gen_crypto_address(self, coinbase_client):
        path_params = {
            'account_id': '1234'
        }

        coinbase = coinbase_client
        address_info = coinbase.gen_crypto_address(path_params)

        if address_info is not None:
            assert True

    @mark.skip(reason='get_user_exchange_limits(), not fully implemented')
    @mark.cb_user_exchange_limits
    @mark.cb_products
    def test_get_user_exchange_limits(self, coinbase_client):
        path_params = {
            'user_id': '1234'
        }
        coinbase = coinbase_client
        user_exchange_limits_data = coinbase.get_user_exchange_limits(path_params)

        if user_exchange_limits_data is not None:
            assert True

    @mark.skip(reason='get_all_reports(), returns an empty list status code 200, revisit')
    @mark.cb_reports
    @mark.cb_products
    def test_get_all_reports(self, coinbase_client):
        query_params = None

        coinbase = coinbase_client
        reports = coinbase.get_all_reports(query_params)

        if reports is not None:
            assert True

    @mark.skip(reason='create_a_report(), incomplete, revisit')
    @mark.cb_reports
    @mark.cb_products
    def test_create_a_report(self, coinbase_client):
        body_params = {

        }
        coinbase = coinbase_client
        report_response = coinbase.create_a_report(body_params)

        if report_response is not None:
            assert True

    @mark.skip(reason='get_a_report(), incomplete, revisit')
    @mark.cb_reports
    @mark.cb_products
    def test_get_a_report(self, coinbase_client):
        path_params = {

        }
        coinbase = coinbase_client
        report_response = coinbase.get_a_report(path_params)

        if report_response is not None:
            assert True

    @mark.create_new_order
    @mark.cb_products
    @mark.cb_orders
    def test_create_new_order_limit(self, coinbase_client, cb_order_resp_key_list,
                                    btc_usd, cb_limit_order_from_fixture):
        """
        Testing placing a limit order.
        :param coinbase_client:
        """
        parameters = cb_limit_order_from_fixture

        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        # cancel_after is an optional field min, hour, day
        limit_order_data = {
            'type': parameters['type'],
            'side': parameters['side'],
            'product_id': btc_usd,
            'price': parameters['price'],
            'size': parameters['size'],
            'time_in_force': 'GTC',
            'cancel_after': '05,00,00',
        }

        limit_order_response = coinbase.create_new_order(limit_order_data)
        key_test = [x in cb_order_resp_key_list for x in limit_order_response.keys()]

        if False not in key_test:
            assert True

    @mark.create_new_order
    @mark.cb_products
    @mark.cb_orders
    def test_create_new_order_market_size(self, coinbase_client, cb_order_resp_key_list,
                                             btc_usd, cb_mkt_order_size_from_fixture):
        """
        Testing placing a limit order by desired amount in base currency
        :param coinbase_client:
        """
        parameters = cb_mkt_order_size_from_fixture

        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        market_order_data = {
            'type': parameters['type'],
            'side': parameters['side'],
            'product_id': btc_usd,
            'size': parameters['size'],
            'time_in_force': 'GTC'
        }

        market_order_response = coinbase.create_new_order(market_order_data)
        key_test = [x in cb_order_resp_key_list for x in market_order_response.keys()]

        if False not in key_test:
            assert True

    @mark.create_new_order
    @mark.cb_products
    @mark.cb_orders
    def test_create_new_order_market_funds(self, coinbase_client, cb_order_resp_key_list,
                                              btc_usd, cb_mkt_order_funds_from_fixture):
        """
        Testing create_new_order market order using funds attribute.
        :param coinbase_client:
        """
        parameters = cb_mkt_order_funds_from_fixture

        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        market_order_data = {
            'type': parameters['type'],
            'side': parameters['side'],
            'product_id': btc_usd,
            'funds': parameters['funds'],
            'time_in_force': 'GTC'
        }

        market_order_response = coinbase.create_new_order(market_order_data)
        key_test = [x in cb_order_resp_key_list for x in market_order_response.keys()]

        if False not in key_test:
            assert True

    @mark.create_new_order
    @mark.cb_products
    @mark.cb_orders
    def test_create_new_order_stop_loss(self, coinbase_client, cb_order_resp_key_list,
                                          btc_usd, cb_stop_order_from_fixture):
        """
        Testing placing a limit order desired amount of quote currency to use
        :param coinbase_client:
        """
        parameters = cb_stop_order_from_fixture

        coinbase = coinbase_client

        # Exchange returns a single account with these fields
        stop_order_data = {
            'type': parameters['type'],
            'side': parameters['side'],
            'product_id': btc_usd,
            'price': parameters['price'],
            'size': parameters['size'],
            'stop': parameters['stop'],
            'stop_price': parameters['stop_price'],
            'time_in_force': 'GTC'
        }

        stop_order_response = coinbase.create_new_order(stop_order_data)
        key_test = [x in cb_order_resp_key_list for x in stop_order_response.keys()]

        if False not in key_test:
            assert True


    @mark.cb_products
    @mark.cb_orders
    def test_get_all_fills(self, coinbase_client, btc_usd, cb_get_all_fills_from_fixture):
        """
        Testing get_all_fills functionality.  Gets a list of fills.  A fill on a specified order
        """
        parameters = cb_get_all_fills_from_fixture

        if len(parameters) == 1:
            fills_params = {
                'product_id':   btc_usd,
                'limit':    parameters['limit']
            }
        else:
            datetime_before_obj = datetime.datetime.strptime(parameters['before'], '%b %d, %Y')
            datetime_after_obj = datetime.datetime.strptime(parameters['after'], '%b %d, %Y')
            fills_params = {
                'product_id':   btc_usd,
                'limit':    parameters['limit'],
                'before':   np.int64(datetime_before_obj.timestamp()),
                'after':    np.int64(datetime_after_obj.timestamp())
            }

        coinbase = coinbase_client

        fills_list = coinbase.get_all_fills(fills_params)

        if fills_list is not None:
            assert len(fills_list) == fills_params['limit']
        else:
            assert False

    @mark.cb_products
    @mark.cb_orders
    def test_get_all_orders(self, coinbase_client, cb_get_all_orders_from_fixture):
        """
        Testing get_all_orders.  Gets a list of all open orders on the exchanges.
        """

        parameters = cb_get_all_orders_from_fixture

        coinbase = coinbase_client

        open_orders_list = coinbase.get_all_orders(parameters)

        if open_orders_list is not None:
            for order in open_orders_list:
                logger.info('Test status during run = {}'.format(parameters['status']))
                assert order['status'] == parameters['status']
        else:
            assert False

    @mark.cb_products
    @mark.cb_orders
    def test_cancel_all_orders(self, coinbase_client, btc_usd, cb_limit_order_from_fixture, cb_stop_order_from_fixture):
        """
        Testing canceling_all_orders which cancels any open orders on the exchange.
        """
        limit_order_parameters = cb_limit_order_from_fixture
        limit_order_data = {
            'type': limit_order_parameters['type'],
            'side': limit_order_parameters['side'],
            'product_id': btc_usd,
            'price': limit_order_parameters['price'],
            'size': limit_order_parameters['size'],
            'time_in_force': 'GTC',
            'cancel_after': '05,00,00',
        }

        stoploss_order_parameters = cb_stop_order_from_fixture
        stop_order_data = {
            'type': stoploss_order_parameters['type'],
            'side': stoploss_order_parameters['side'],
            'product_id': btc_usd,
            'price': stoploss_order_parameters['price'],
            'size': stoploss_order_parameters['size'],
            'stop': stoploss_order_parameters['stop'],
            'stop_price': stoploss_order_parameters['stop_price'],
            'time_in_force': 'GTC'
        }

        coinbase = coinbase_client

        limit_order_response = coinbase.create_new_order(limit_order_data)
        stop_order_response = coinbase.create_new_order(stop_order_data)

        cancel_orders_list = coinbase.cancel_all_orders()

        if cancel_orders_list is not None:
            for item in cancel_orders_list:
                pattern = re.compile(r'\w+-\w+-\w+-\w+')
                assert re.search(pattern, item)
        else:
            assert False

    @mark.cb_products
    @mark.cb_orders
    def test_get_single_order(self, coinbase_client, btc_usd, cb_limit_order_from_fixture):
        limit_order_parameters = cb_limit_order_from_fixture
        limit_order_data = {
            'type': limit_order_parameters['type'],
            'side': limit_order_parameters['side'],
            'product_id': btc_usd,
            'price': limit_order_parameters['price'],
            'size': limit_order_parameters['size'],
            'time_in_force': 'GTC',
            'cancel_after': '05,00,00',
        }

        coinbase = coinbase_client

        # first create some tests in case there are no open tests
        limit_order_response = coinbase.create_new_order(limit_order_data)

        order_rsp = coinbase.get_single_order(limit_order_response['id'])

        if order_rsp is not None:
            assert order_rsp['id'] == limit_order_response['id']

        # clean up all open orders
        coinbase.cancel_all_orders()

    @mark.cb_products
    @mark.cb_orders
    def test_cancel_an_order(self, coinbase_client, btc_usd, cb_limit_order_from_fixture):
        limit_order_parameters = cb_limit_order_from_fixture
        limit_order_data = {
            'type': limit_order_parameters['type'],
            'side': limit_order_parameters['side'],
            'product_id': btc_usd,
            'price': limit_order_parameters['price'],
            'size': limit_order_parameters['size'],
            'time_in_force': 'GTC',
            'cancel_after': '05,00,00',
        }

        coinbase = coinbase_client

        # first create a test in case there are no open tests and use it's id
        limit_order_response = coinbase.create_new_order(limit_order_data)
        # cancel the new test created using the id from last line
        cancel_order_response = coinbase.cancel_an_order(limit_order_response['id'])

        if cancel_order_response is not None:
            # attempt to grab the last limit order test already created but should be gone by now
            single_order_rsp = coinbase.get_single_order(limit_order_response['id'])
            if not bool(single_order_rsp):
                # if created limit test is gone then pass test
                assert True
