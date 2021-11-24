from pathlib import Path
from requests.auth import AuthBase
from threading import Thread
import json, hmac, hashlib, time, requests, base64
import logging
import os
import websocket
import ssl

logger = logging.getLogger()


class CoinbaseClient:
    """
    Coinbase API, with REST and Websocket hooks to exchange
    """
    def __init__(self, sandbox):
        if sandbox:
            self.base_url = 'https://api-public.sandbox.pro.coinbase.com'
            self.wss_url = 'wss://ws-feed-public.sandbox.pro.coinbase.com'
            self.auth = CoinbaseExchangeAuth(sandbox=True)
            logger.info('Coinbase Client Sandbox successfully initialized')
        else:
            self.base_url = 'https://api-public.pro.coinbase.com'
            self.wss_url = 'wss://ws-feed.pro.coinbase.com'
            self.auth = CoinbaseExchangeAuth(sandbox=False)
            logger.info('Coinbase Client successfully initialized')

        self.prices = dict()
        self.id = 1
        self.ws = None
        # t = Thread(target=self.start_ws)
        # t.start()

    def get_single_account_ledger(self, acct_id):
        """
        Lists ledger activity for an account. This includes anything that would affect the accounts
         balance - transfers, trades, fees, etc.
        """
        request = self.make_request('GET', '/accounts/{}/ledger'.format(acct_id), None)

        account_history = []

        if request is not None:
            for hist in request:
                data = {
                    'id': hist['id'],
                    'amount': float(hist['amount']),
                    'balance': float(hist['balance']),
                    'created_at': hist['created_at'],
                    'type': hist['type'],
                    'details': {'order_id': hist['details']['order_id'],
                                'product_id': hist['details']['product_id'],
                                'trade_id': hist['details']['trade_id']
                                }
                }
                account_history.append(data)

        return account_history

    def get_single_account_transfers(self, acct_id):
        """
        Lists past withdrawals and deposits for an account.
        """
        request = self.make_request('GET', '/accounts/{}/transfers'.format(acct_id), None)

        account_history = []

        if request is not None:
            for hist in request:
                data = {
                    'id': hist['id'],
                    'amount': float(hist['amount']),
                    'balance': float(hist['balance']),
                    'created_at': hist['created_at'],
                    'type': hist['type'],
                    'details': {'order_id': hist['details']['order_id'],
                                'product_id': hist['details']['product_id'],
                                'trade_id': hist['details']['trade_id']
                                }
                }
                account_history.append(data)

        return account_history

    def cancel_all_orders(self):
        """
        With best effort, cancel all open orders from the profile that the API key belongs to.
        :return: List of ids of the canceled orders
        """
        exchange_response = self.make_request('DELETE', '/orders', None)

        if exchange_response is not None:
            logger.info('{} open orders canceled'.format(len(exchange_response)))

        return exchange_response

    def cancel_an_order(self, order_id):
        """
        cancel a specific order using order_id of active order
        """
        exchange_response = self.make_request('DELETE', '/orders/{}'.format(order_id), None)
        if exchange_response is not None:
            logger.info('order_id {} was canceled'.format(order_id))

        return exchange_response

    def get_single_account_id(self, acct_id):
        """
        Information for a single account. Use this endpoint when you know the account_id.
        API key must belong to the same profile as the account.
        :param acct_id:
        :return:
        """
        response = self.make_request('GET', '/accounts/{}'.format(acct_id), None)

        single_account_id_data = {}

        if response is not None:
            single_account_id_data = {
                'id': response['id'],
                'currency': response['currency'],
                'balance': float(response['balance']),
                'available': float(response['available']),
                'hold': float(response['hold']),
                'profile_id': response['profile_id'],
                'trading_enabled': response['trading_enabled']
            }
            return single_account_id_data
        return None

    def get_single_order(self, order_id):
        """
        Get a single order by order id from the profile that the API key belongs to.
        :param order_id:
        :return:
        """
        response = self.make_request('GET', '/orders/{}'.format(order_id), None)

        order_data = dict()

        if response is not None:
            order_data = {
                'id': response['id'],
                'size': float(response['size']),
                'product_id': response['product_id'],
                'side': response['side'],
                # 'stp': exchange_info['stp'],
                # 'funds': float(exchange_info['funds']),
                # 'specified_funds': float(exchange_info['specified_funds']),
                'type': response['type'],
                'post_only': response['post_only'],
                'created_at': response['created_at'],
                # 'done_at': exchange_info['done_at'],
                # 'done_reason': exchange_info['done_reason'],
                'fill_fees': float(response['fill_fees']),
                'filled_size': float(response['filled_size']),
                'executed_value': float(response['executed_value']),
                'status': response['status'],
                'settled': response['settled']
            }

        return order_data

    def get_api_time(self):
        """
        Get the API server time.
        :return time_data:
        """
        response = self.make_request('GET', '/time', None)

        time_data = dict()

        if response is not None:
            time_data = {
                'iso': response['iso'],
                'epoch': float(response['epoch'])
            }

        return time_data

    def get_balances(self):
        """
        Gets a list of balances of accounts with a balance greater than zero.
        :return:
        """
        accounts_data = self.get_all_accounts()

        balance_data = []

        if accounts_data is not None:
            for account in accounts_data:
                data = dict()
                if float(account['balance']) > 0:
                    data = {
                        'id': account['id'],
                        'currency': account['currency'],
                        'balance': float(account['balance']),
                        'hold': float(account['hold']),
                        'available': float(account['available']),
                        'profile_id': account['profile_id'],
                        'trading_enabled': account['trading_enabled']
                    }
                    balance_data.append(data)
                else:
                    continue

        return balance_data

    def get_bid_ask(self, symbol, level=1):
        """
        Get a list of open orders for a product.  Amount of detail custom by level param..1,2,3
        available
        """
        data = dict()
        data['id'] = symbol
        ob_data = self.get_product_ob(symbol, level)
        # TODO this section below is not right.  If level is set higher than 1 code below will fail
        if ob_data is not None:
            if symbol not in self.prices:
                self.prices[symbol] = {'bid': float(ob_data['bids'][0][0]),
                                       'ask': float(ob_data['asks'][0][0])}
            else:
                self.prices[symbol]['bid'] = float(ob_data['bids'][0][0])
                self.prices[symbol]['ask'] = float(ob_data['asks'][0][0])

        return self.prices[symbol]

    def get_bid_ask_latest(self, symbol):
        """
        Snapshot information about the last trade (tick), best bid/ask and 24 volume
        :param symbol:
        :return: prices
        """
        data = dict()
        data['id'] = symbol
        # ob_data = self.make_request('GET', '/products/{}/ticker'.format(data['id']), None)
        ob_data = self.get_product_ticker(symbol)

        # TODO complete remaining ob_data is a dict with keys: trade_id, price, size, time, bid,
        #  ask, volume
        '''
        if ob_data is not None:
            if symbol not in self.prices:
                self.prices[symbol] = {'bid': float(ob_data['bids'][0][0]),
                                        'ask': float(ob_data['asks'][0][0])}
            else:
                self.prices[symbol]['bid'] = float(ob_data['bids'][0][0])
                self.prices[symbol]['ask'] = float(ob_data['asks'][0][0])
        '''
        return self.prices[symbol]

    def get_contracts(self):
        """
        Same information as get_products but broken out by tradeable security
        :return: contracts
        """
        products = self.get_products()

        contracts = dict()

        if products is not None:
            for contract_data in products:
                contracts[contract_data['id']] = contract_data

        return contracts

    def convert_currency(self, params):
        """
        Converts funds from from currency to to currency.
        Funds are converted on the from account in the profile_id profile.
        """
        response = self.make_request('POST', '/conversions', params)

        data = {}

        if response is not None:
            return data

        return None

    def _convert_currency_dict(self, currency):
        """
        Helper method for get currency methods used to response dictionary values from exchange to
        desired formats
        """

        currency_data = {
            'id': currency['id'],
            'name': currency['name'],
            'min_size': float(currency['min_size']),
            'status': currency['status'],
            'message': currency['message'],
            'max_precision': float(currency['max_precision']),
            'convertible_to': currency['convertible_to'],
            'details': {
                'type': currency['details']['type'] \
                    if currency['details']['type'] is not None else '',
                'symbol': currency['details']['symbol'] \
                    if currency['details']['symbol'] is not None else '',
                'network_confirmations': int(currency['details']['network_confirmations']) \
                    if currency['details']['network_confirmations'] is not None else '',
                'sort_order': int(currency['details']['sort_order']) \
                    if currency['details']['sort_order'] is not None else '',
                'crypto_address_link': currency['details']['crypto_address_link'] \
                    if currency['details']['crypto_address_link'] is not None else '',
                'crypto_transaction_link': currency['details']['crypto_transaction_link'] \
                    if currency['details']['crypto_transaction_link'] is not None else '',
                'push_payment_methods': currency['details']['push_payment_methods'] \
                    if currency['details']['push_payment_methods'] is not None else '',
                'group_types': currency['details']['group_types'] \
                    if currency['details']['group_types'] is not None else '',
                'display_name': currency['details']['display_name'] \
                    if currency['details']['display_name'] is not None else '',
                'processing_time_seconds': int(currency['details']['processing_time_seconds']) \
                    if currency['details']['processing_time_seconds'] is not None else '',
                'min_withdrawal_amount': float(currency['details']['min_withdrawal_amount']) \
                    if currency['details']['min_withdrawal_amount'] is not None else '',
                'max_withdrawal_amount': int(currency['details']['max_withdrawal_amount']) \
                    if currency['details']['max_withdrawal_amount'] is not None else ''
            }
        }

        return currency_data

    def get_all_cb_wallets(self):
        """
        Gets all the user's available Coinbase wallets (These are the wallets/accounts that are
        used for buying and selling on www.coinbase.com)
        """
        response = self.make_request('GET', '/coinbase-accounts', None)

        wallet_list = []

        if response is not None:
            for wallet in response:
                wallet_data = {
                    'id': wallet['id'],
                    'name': wallet['name'],
                    'balance': float(wallet['balance']),
                    'currency': wallet['currency'],
                    'type': wallet['type'],
                    'primary': wallet['primary'],
                    'active': wallet['active'],
                    'available_on_consumer': wallet['available_on_consumer'],
                    'hold_balance': float(wallet['hold_balance']),
                    'hold_currency': wallet['hold_currency']
                }
                wallet_list.append(wallet_data)
            return wallet_list

        return None

    def get_all_currencies(self):
        """
        Gets a list of all known currencies.
        Note: Not all currencies may be currently in use for trading.
        """

        response = self.make_request('GET', '/currencies', None)

        currencies_data = []

        if response is not None:
            for currency in response:
                currencies_data.append(self._convert_currency_dict(currency))
            return currencies_data

        return None

    def get_a_currency(self, currency_id):
        """
        Gets a single currency by id.
        """
        # TODO same as with get_currencies method
        currency = self.make_request('GET', '/currencies/{}'.format(currency_id), None)
        if currency is not None:
            return self._convert_currency_dict(currency)

        return None

    def get_a_report(self, params):
        """
        Get a specific report by report_id.
        """
        response = self.make_request('GET', '/reports/{}'.format(params['report_id']), None)

        report_data = {}

        if response is not None:
            return report_data

        return None

    def get_a_conversion(self, path_params, params):
        """
        Gets a currency conversion by id (i.e. USD -> USDC).
        """
        response = self.make_request('GET', '/conversions/{}'.format(path_params['conversion_id']), params)

        data = {}

        if response is not None:
            return data

        return None



    def get_fees(self):
        """
        Get fees rates and 30 days trailing volume.
        """
        response = self.make_request('GET', '/fees', None)

        fees_data = {}

        if response is not None:
            fees_data = {
                'taker_fee_rate': float(response['taker_fee_rate']),
                'maker_fee_rate': float(response['maker_fee_rate']),
                'usd_volume': float(response['usd_volume'])
            }
            return fees_data

        return None

    def get_historic_rates(self, symbol, interval):
        """
        Historic rates for a product. Rates are returned in grouped buckets based on requested
        granularity. Granularity must be 60, 300, 900, 3600, 21600, 86400...1min, 5min, 15min,
        1hr, 6hr, 1day. So 2 day is 86400 * 2
        """
        data = dict()
        data['symbol'] = symbol
        data['interval'] = interval

        response = self.make_request('GET', '/products/{}/candles'.format(data['symbol']), None)
        candles = []

        if response is not None:
            for c in response:
                #              time,    low,         high,        open,       close,        volume
                candles.append([c[0], float(c[1]), float(c[2]), float(c[3]), float(c[4]),
                                float(c[5])])

        return candles

    def get_single_account_holds(self, acct_id, params=None):
        """
        List holds of an account that belong to the same profile as the API key.
        Holds are placed on an account for any active orders or pending withdraw requests. As an
        order is filled, the hold amount is updated. If an order is canceled, any remaining hold
        is removed. For a withdraw, once it is completed, the hold is removed.
        """
        response = self.make_request('GET', '/accounts/{}/holds'.format(acct_id), params)

        holds_data = []

        if response is not None:
            for hold in response:
                hold_data = {
                    'id': hold['id'],
                    'created_at': hold['created_at'],
                    'updated_at': hold['updated_at'],
                    'type': hold['type'],
                    'ref': hold['ref']
                }
                holds_data.append(hold_data)
        return holds_data

    def get_products(self):
        """
        Get a list of available currency pairs for trading.
        :return: products
        """
        response = self.make_request('GET', '/products', None)

        products_list = []

        if response is not None:
            for product in response:
                data = self.make_product_dict(product)
                products_list.append(data)
        else:
            return None

        return products_list

    def get_product_ob(self, symbol, params):
        """
        Get a list of open orders for a product. The amount of detail shown can be customized
        with the level parameter.
        """
        level = int(params['level'])
        response = self.make_request('GET', '/products/{}/book'.format(symbol), params)

        ob_data = dict()
        bid_list = []
        ask_list = []

        if response is not None:
            for key in response.keys():
                if not key == 'sequence':
                    ob_data[key] = self.make_product_ob_list(response[key], level)
            ob_data['sequence'] = float(response['sequence'])
        else:
            return None

        return ob_data

    def get_product_ticker(self, symbol):
        """
        Snapshot information about the last trade (tick), best bid/ask and 24h volume.
        :param symbol:
        :return:
        """
        response = self.make_request('GET', '/products/{}/ticker'.format(symbol), None)

        product_data = dict()

        if response is not None:
            product_data = {
                'trade_id': int(response['trade_id']),
                'price': float(response['price']),
                'size': float(response['size']),
                'time': response['time'],
                'bid': float(response['bid']),
                'ask': float(response['ask']),
                'volume': float(response['volume'])
            }
        else:
            return None

        return product_data

    def _get_profile_data(self, profile):
        """
        Used to convert exchange data response to a dictionary
        :param profile:
        :return:
        """
        profile_data = {
            'id': profile['id'],
            'user_id': profile['user_id'],
            'name': profile['name'],
            'active': profile['active'],
            'is_default': profile['is_default'],
            # TODO created_at is data-time maybe convert from str to datetime
            'created_at': profile['created_at']
        }
        return profile_data

    def get_profiles(self):
        """
        Gets a list of all of the current user's profiles.
        """
        response = self.make_request('GET', '/profiles', None)

        profile_list = []

        if response is not None:
            for profile in response:
                profile_data = self._get_profile_data(profile)
                profile_list.append(profile_data)
            return profile_list

        return None

    def get_profile_by_id(self, profile_id):
        """
        Information for a single profile. Use this endpoint when you know the profile_id.
        """
        response = self.make_request('GET', '/profiles/{}'.format(profile_id), None)

        profile_data = {}

        if response is not None:
            profile_data = self._get_profile_data(response)
            return profile_data

        return None

    def get_single_product(self, symbol):
        """
        Get market data for a specific currency pair.

        :param symbol:
        :return:
        """
        product = self.make_request('GET', '/products/{}'.format(symbol), None)

        data = dict()

        if product is not None:
            data = self.make_product_dict(product)
        else:
            return None

        return data

    def get_signed_prices(self):
        """
        Get cryptographically signed prices ready to be posted on-chain using Compound's Open
        Oracle smart contract.
        """
        response = self.make_request('GET', '/oracle', None)

        data = {}

        if response is not None:
            data = {
                'timestamp': response['timestamp'],
                'messages': response['messages'],
                'signatures': response['signatures'],
                'prices': response['prices']
            }
        else:
            return None

        return data

    def get_trades(self, symbol):
        """
        List the latest trades for a product.
        :param symbol:
        :return:
        """
        response = self.make_request('GET', '/products/{}/trades'.format(symbol), None)

        trade_data = []

        if response is not None:
            for trade in response:
                data = {
                    'time': trade['time'],
                    'trade_id': trade['trade_id'],
                    'price': float(trade['price']),
                    'size': float(trade['size']),
                    'side': trade['side']
                }
                trade_data.append(data)
        else:
            return None

        return trade_data

    def get_user_exchange_limits(self, params):
        """
        Gets exchange limits information for a single user.
        """
        response = self.make_request('GET', '/users/{}/exchange-limits'.format(params['user_id']), None)

        exchange_limit_data = {}

        if response is not None:
            return exchange_limit_data

        return None

    def gen_crypto_address(self, params=None):
        """
        Generates a one-time crypto address for depositing crypto.
        """
        response = self.make_request('POST', '/coinbase-accounts/{}/addresses'.format(params['id'])
                                     , None)

        address_data = {}

        if response is not None:
            return address_data

        return None

    def get_24hr_stats(self, symbol):
        """
        Get 24 hr stats for the product. volume is in base currency units. open, high,
        low are in quote currency units.
        :param symbol: product_id
        :return: stats of the last 24 hours
        """
        stats = dict()
        response = self.make_request('GET', '/products/{}/stats'.format(symbol), None)

        if response is not None:
            stats = {
                'open': float(response['open']),
                'high': float(response['high']),
                'low': float(response['low']),
                'volume': float(response['volume']),
                'last': float(response['last']),
                'volume_30day': float(response['volume_30day'])
            }
        else:
            return None

        return stats

    def get_all_accounts(self):
        """
        Get a list of trading accounts from the profile of the API key.
        :return:
        """
        response = self.make_request('GET', '/accounts', None)

        account_list = []

        if response is not None:
            for resp in response:
                data = {
                    'id': resp['id'],
                    'currency': resp['currency'],
                    'balance': float(resp['balance']),
                    'available': float(resp['available']),
                    'hold': float(resp['hold']),
                    'profile_id': resp['profile_id'],
                    'trading_enabled': resp['trading_enabled']
                }
                account_list.append(data)

        return account_list

    def get_all_fills(self, params=None):
        """
        Get a list of recent fills of the API key's profile.
        """
        response = self.make_request('GET', '/fills', params)

        if response is not None:
            return response

        return None

    def get_all_orders(self, params):
        """
        List your current open orders from the profile that the API key belongs to.
        :return:
        """
        response = self.make_request('GET', '/orders', params)

        orders_list = []

        if response is not None:
            for order in response:
                data = {
                    'id': order['id'],
                    'price': float(order['price']),
                    'size': float(order['size']),
                    'product_id': order['product_id'],
                    'profile_id': order['profile_id'],
                    'side': order['side'],
                    'type': order['type'],
                    'time_in_force': order['time_in_force'],
                    'post_only': order['post_only'],
                    'created_at': order['created_at'],
                    'fill_fees': float(order['fill_fees']),
                    'filled_size': float(order['filled_size']),
                    'executed_value': float(order['executed_value']),
                    'status': order['status'],
                    'settled': order['settled']
                }
                orders_list.append(data)
            return orders_list

        return None

    def get_all_reports(self, params=None):
        """
        Gets a list of past fills/account reports.
        """
        # TODO, currently response.status code == 200 response is an empty list using sandbox env
        response = self.make_request('GET', '/reports', params)

        reports_data = {}

        if response is not None:
            return reports_data

        return None

    def make_product_dict(self, product):
        """
        Method used to convert product exchange data and cast specific keys
        """

        product_dict = {
            'id': product['id'],
            'base_currency': product['base_currency'],
            'quote_currency': product['quote_currency'],
            'base_min_size': float(product['base_min_size']),
            'base_max_size': float(product['base_max_size']),
            'quote_increment': float(product['quote_increment']),
            'base_increment': float(product['base_increment']),
            'display_name': product['display_name'],
            'min_market_funds': float(product['min_market_funds']),
            'max_market_funds': float(product['max_market_funds']),
            'margin_enabled': product['margin_enabled'],
            'fx_stablecoin': product['fx_stablecoin'],
            'post_only': product['post_only'],
            'limit_only': product['limit_only'],
            'cancel_only': product['cancel_only'],
            'trading_disabled': product['trading_disabled'],
            'status': product['status'],
            'status_message': product['status_message']
        }

        return product_dict

    def make_product_ob_list(self, response, level):
        """
        Helper method to convert order book info from exchange to float values where appropriate
        :param response: part of the order book data response from the exchange
        :param level: depth of the order book level 1, 2, or 3
        :return: order book data as a list
        """
        ob_list = []

        for line in response:
            tmp_list = []
            for i, elem in enumerate(line):
                if i == 2 and level == 3:
                    # this should be the 'sequence' key
                    tmp_list.append(elem)
                else:
                    tmp_list.append(float(elem))
            ob_list.append(tmp_list)

        return ob_list

    def make_request(self, method, endpoint, data):
        """
        Method used to send request commands to exchange.
        """
        # Coinbase doesn't like mixing str and int so cast data an str
        headers = {'Accept': 'application/json'}
        if method == 'DELETE':
            response = requests.delete(self.base_url + endpoint, headers=headers,
                                       params=str(data), auth=self.auth)
        elif method == 'GET':
            response = requests.get(self.base_url + endpoint, headers=headers, params=data,
                                    auth=self.auth)
        elif method == 'POST':
            headers = {
                'Accept': 'application/json',
                'Content-type': 'application/json'
            }
            response = requests.post(self.base_url + endpoint, headers=headers, json=data,
                                     auth=self.auth)
        else:
            raise ValueError()

        if response.status_code == 200:
            return response.json()
        else:
            logger.error("Error while making %s request to %s (error code %s) response from server:"
                         "%s", method, endpoint, response.status_code, response.text)
        return None

    def create_new_order(self, order):
        """
        You can place two types of orders: limit and market. Orders can only be placed if your
        account has sufficient funds. Each profile can have a maximum of 500 open orders on a
        product. Once reached, the profile will not be able to place any new orders until the
        total number of open orders is below 500. Once an order is placed, your account funds
        will be put on hold for the duration of the order. How much and which funds are put on
        hold depends on the order type and parameters specified. See the Holds details below.
        """

        # TODO fix order_data dict, since market and limit are slightly different best to combine
        #  them
        order_data = {}
        response = self.make_request('POST', '/orders', order)

        if response is not None:
            order_data = {
                'id': response['id'],
                'product_id': response['product_id'],
                'side': response['side'],
                'stp': response['stp'],
                'type': response['type'],
                'post_only': response['post_only'],
                'created_at': response['created_at'],
                'fill_fees': float(response['fill_fees']),
                'executed_value': float(response['executed_value']),
                'status': response['status'],
                'settled': response['settled']
            }
            if 'market' in order:
                if 'size' in order:
                    order_data = {
                        'funds': float(response['funds']),
                        'size': float(response['size']),
                    }
                elif 'funds' in order:
                    order_data = {
                        'funds': float(response['funds']),
                        'specified_funds': float(response['specified_funds']),
                        'filled_size': response['filled_size'],
                    }
            elif 'limit' in order:
                order_data = {
                    'price': float(response['price']),
                    'size': float(response['size']),
                    'time_in_force': response['time_in_force'],
                    'filled_size': float(response['filled_size']),
                }
            return order_data

        return None

    def create_a_profile(self, params):
        """
        Create a new profile. Will fail if no name is provided or if user already has 10 profiles.
        """
        response = self.make_request('POST', '/profiles', params)

        profile_data = {}

        if response is not None:
            return profile_data
        else:
            return None

    def create_a_report(self, params=None):
        """
        Generates a report. Reports are either for past account history or past fills on either
        all accounts or one product's account.
        """
        response = self.make_request('POST', '/reports', params)

        report_data = {}

        if response is not None:
            return report_data

        return None

    def delete_a_profile(self, path_param, params):
        """
        Deletes the profile specified by profile_id and transfers all funds to the profile
        specified by to. Fails if there are any open orders on the profile to be deleted.
        """
        # TODO need to resolve create_a_profile() before can fully test this method
        response = self.make_request('POST', '/profiles/{}/deactivate'.format(path_param['profile_id']), params)

        if response is not None:
            return response

        return None

    def start_ws(self):
        self.ws = websocket.WebSocketApp(self.wss_url,
                                         on_open=self.on_open,
                                         on_close=self.on_close,
                                         on_error=self.on_error,
                                         on_message=self.on_message)
        self.ws.run_forever()
        # self.ws.run_forever(sslopt={'cert_reqs': ssl.CERT_NONE})

    def on_open(self, ws):
        logger.info('Coinbase websocket connection opened')

    def on_close(self, ws):
        logger.warning('Coinbase websocket connection closed')

    def on_error(self, ws, msg):
        logger.error('Coinbase connection error: %s', msg)

    def on_message(self, ws, msg):
        print(msg)

    def subscribe_channel(self, symbol):
        return

    def rename_a_profile(self, path_param, params):
        """
        Rename a profile. Names 'default' and 'margin' are reserved.
        """
        # TODO incomplete, create_a_profile() is not working revisit this method after resolving
        response = self.make_request('POST', '/profiles/{}'.format(path_param['profile_id']), params)

        profile_data = {}

        if response is not None:
            profile_data = self._get_profile_data(response)
            return profile_data

        return None

    def transfer_funds_between_profiles(self, params):
        """
        Transfer an amount of currency from one profile to another.
        """
        # TODO incomplete, create_a_profile() is not working revisit this method after resolving
        response = self.make_request('POST', '/profiles/transfer', params)

        if response is not None:
            return response

        return None



# Create custom authentication for Exchange
class CoinbaseExchangeAuth(AuthBase):
    """
    Class used to authenticate a connection to Coinbase exchange
    """
    def __init__(self, sandbox):
        if sandbox:
            api_info = self.get_api_key_info(sandbox)
        else:
            api_info = self.get_api_key_info(sandbox=False)

        self.api_key = api_info['api_key']
        self.api_secret = api_info['api_secret']
        self.api_pass = api_info['api_pass']

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.api_secret)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.api_pass,
            'Content-Type': 'application/json'
        })
        return request

    def get_api_key_info(self, sandbox):
        '''
        crypto_api:
        API_KEY
        API_SECRET
        API_PASS
        '''
        api_list = []
        filename = ''
        quant_dir = '{}/workspace_local/quant'.format(str(Path.home()))

        if sandbox:
            filename = 'coinbase_api_sandbox.txt'
        else:
            filename = 'coinbase_api.txt'

        os.chdir(quant_dir)
        with open('accounts/{}'.format(filename), 'r') as file_api:
            for line in file_api:
                # assign to list but remove newline ascii at end
                api_list.append(line.rstrip())
        file_api.close()

        return {'api_key': api_list[0], 'api_secret': api_list[1], 'api_pass': api_list[2]}
