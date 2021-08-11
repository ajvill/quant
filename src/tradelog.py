"""
TradeLog: Creates, Updates, Maintains trade details and performance data.  Inputs files such as CSV
        converts into custom format which can be outputted

@author: Alberto Villarreal
"""
import re
import numpy as np
import pandas as pd
import datetime


class TradeLog:
    """
    This class takes a formatted input creates a trade log
    """
    # Class Object Attribute
    trade_log_headers = ['Date', 'Trade_Type', 'Action', 'Underlying', 'Series', 'Trade_Description',
                         'Quantity', 'Points_AvgPrice', 'NetCost_Credit', 'Net_ExitPrice', 'Exit_Date',
                         'W_L', 'Realized_Gain', 'Realized_Gain_Percent', 'Days_Held', 'Comments']

    def __init__(self, infile):
        # Class Variables
        self.infile = infile
        # creates a TradeLogFormat object
        self.tl_format = self.get_tl_format_object()
        # Local copy of the initial TradeLog dataframe
        self.df = self.tl_format.df
        # stock_trade dataframe
        self.stock_trades = pd.DataFrame()
        # option trade dataframe
        self.option_trades = pd.DataFrame()
        # securities list
        self.stock_trade_log_list = []
        self.option_trade_log_list = []
        # TradeLog dataframe
        self.trade_log_df = pd.DataFrame()

        # runs TradeLog methods
        self.setup()

    def setup(self):
        self.sort_df()
        self.get_stock_trades()
        self.get_option_trades()
        self.create_stock_trade_transactions_list()
        self.create_option_trade_transactions_list()
        self.create_trade_log_df()
        self.write_tl_to_csv()
        print("Test")

    def create_option_trade_transactions_list(self):
        stock_tickers = self.option_trades['Underlying'].unique()
        option_trades_grouped = self.option_trades.groupby(['Underlying', 'Date', 'Action', 'Trade_Description']).sum()
        trade_complete_list = []
        print("Test")

    def create_stock_trade_transactions_list(self):
        """
        This method will create stock trade transactions to be merged into the trade_log dataframe.
        :return: updates class variable stock_trade_log_list
        """
        stock_tickers = self.stock_trades['Underlying'].unique()
        stock_trades_grouped = self.stock_trades.groupby(['Underlying', 'Date', 'Action']).sum()
        trade_complete_list = []
        # Trying to parse through list of stock_tickers pulling out the rows based on Underlying
        for ticker in stock_tickers:
            stock_df = stock_trades_grouped.loc[ticker]
            stock_index = stock_df.index
            bot_count = 0
            bot_cost_credit = 0
            bot_date = ''
            sld_count = 0
            sld_cost_credit = 0
            sld_date = ''
            for trade in range(len(stock_df)):
                # stock_index is a tuple: (Date, Action), stock_index[trade]['Action']
                if stock_index[trade][1] == 'Bot':
                    if not bot_date:
                        bot_date = stock_index[trade][0]
                    bot_count += stock_df['Quantity'].iloc[trade]
                    bot_cost_credit += stock_df['Net_Cost/Credit'].iloc[trade]
                else:
                    sld_count += stock_df['Quantity'].iloc[trade]
                    sld_cost_credit += stock_df['Net_Cost/Credit'].iloc[trade]
                    if not self.is_trade_series_valid(bot_count, sld_count):
                        bot_count = 0
                        sld_count = 0
                if self.is_trade_complete(bot_count, sld_count):
                    # Trade transaction complete, record sld_date and create elements for tl_list
                    if not sld_date:
                        sld_date = stock_index[trade][0]
                        tradeType = 'Long'
                        action = 'closed'
                        underlying = ticker
                        series = ''
                        trade_desc = ticker
                        quantity = bot_count
                        avg_price = stock_df['Avg_Price'].iloc[trade] # I think wrong
                        cost_credit = bot_cost_credit
                        exit_date = sld_date
                        exit_cost_credit = sld_cost_credit
                        realized_gain = exit_cost_credit + cost_credit
                        realized_gain_percent = realized_gain/abs(cost_credit)
                        if realized_gain >= 0:
                            trade_outcome = 'W'
                        else:
                            trade_outcome = 'L'
                        days_held = pd.to_datetime(sld_date) - pd.to_datetime(bot_date)

                        trade_complete_list.append([bot_date, tradeType, action, underlying, series, trade_desc,
                                                   quantity, avg_price, cost_credit.round(2), exit_cost_credit.round(2),
                                                    exit_date, trade_outcome, realized_gain.round(2),
                                                    realized_gain_percent.round(4), days_held.days, " "])

                    # reset
                    bot_count = 0
                    bot_cost_credit = 0
                    bot_date = ''
                    sld_count = 0
                    sld_cost_credit = 0
                    sld_date = ''
        # save off list
        self.stock_trade_log_list = trade_complete_list

    def create_trade_log_df(self):
        """
        This method will create a new dataframe out of security lists
        :return: update trade_log_df
        """
        security_list = []
        # extend list with stock and option trade data
        security_list.extend(self.stock_trade_log_list)
        security_list.extend(self.option_trade_log_list)

        # create a dataframe from security_list data
        self.trade_log_df = pd.DataFrame(data=security_list, columns=self.trade_log_headers)

    def get_option_trades(self):
        """
        This method filters out option_trades from the trade log df
        :return: none
        """
        self.option_trades = self.df[self.df['Series'] != '']
        self.option_trades.index = range(len(self.option_trades))

    def get_stock_trades(self):
        """
        This method filters out stock_trades from the trade log df
        :return: none
        """
        self.stock_trades = self.df[self.df['Series'] == '']
        self.stock_trades.index = range(len(self.stock_trades))

    def get_tl_format_object(self):
        """
        This method fetches an object from
        :return:
        """
        return TradeLogFormat(self.infile)

    def is_trade_complete(self, bot_count, sld_count):
        """
        This method returns true or false if a trade transaction completes. That is when
        bot_quantity + sld_quantity = 0.
        :param bot_count:
        :param sld_count:
        :return: trade_complete boolean
        """
        trade_complete = False
        if not (bot_count and sld_count) == 0:
            # pdb.set_trace()
            if bot_count + sld_count == 0:
                trade_complete = True
            elif bot_count + sld_count < 0:
                trade_complete = False
        return trade_complete

    def is_trade_series_valid(self, bot_count, sld_count):
        """
        This method checks if trade transaction is valid SLD quantity should never be larger than Bot
        :param bot_count:
        :param sld_count:
        :return: valid boolean
        """
        # pdb.set_trace()
        valid = True
        if abs(sld_count) > bot_count:
            valid = False
        return valid

    def sort_df(self):
        """
        This method will sort the dataframe by Trade Description (handles both stock
        and option trades) then by Action, then Date in ascending order
        :return: none
        """
        sort_order = ['Trade_Description', 'Action', 'Date']
        self.df = self.df.sort_values(sort_order)
        # realign index
        self.df.index = range(len(self.df))

    def write_tl_to_csv(self):
        """
        This method outputs trade_log_df to csv output file
        :return:
        """
        name = self.infile.split('.')
        pattern = re.compile('(/)(\w+)')
        filename = re.search(pattern, name[0])
        csv_file = '{}_tl.csv'.format(filename.group(2))
        self.trade_log_df.to_csv(csv_file)


class TradeLogFormat:
    """
    This class cleans up input csv data and converts into final TradeLog format.
    """
    def __init__(self, infile):
        self.infile = infile
        self.df = self.create_df_from_csv()
        self.setup_format()

    def setup_format(self):
        """
        This method is called in constructor will run through converting from CSV Fidelity download to
        performance config
        """
        self.remove_unused_columns()
        self.rename_avg_price()
        self.rename_net_cost()
        self.create_trade_type_column()
        self.convert_action_format()
        self.convert_run_date_column()
        self.create_underlying_series_trade_description_columns()
        self.reorder_columns()
        # Fix leading white space for the following..last minute cleanup
        self.df['Date'] = self.df['Date'].str.lstrip()
        self.df['Underlying'] = self.df['Underlying'].str.lstrip()
        self.df['Trade_Description'] = self.df['Trade_Description'].str.lstrip()

    def convert_action_format(self):
        """
        This method runs through the 'Action' data converts to SLD or Bot
        """
        self.df['Action'] = self.df['Action'].replace(to_replace=r'.*(SOLD).*', value='SLD', regex=True)
        self.df['Action'] = self.df['Action'].replace(to_replace=r'.*(BOUGHT).*', value='Bot', regex=True)

        # do some cleanup drop rows that are not Buy or Sell trade transactions
        option_actions = []
        for i, act in enumerate(self.df['Action']):
            if not act == 'Bot' and not act == 'SLD':
                # create a list of indexes to remove from the DataFrame, remove any
                # that are not Bot or SLD actions
                option_actions.append(i)
        self.df = self.df.drop(index=option_actions, axis=0)
        # Reset the index range now that we had removed a row
        self.df.index = range(len(self.df))

    def convert_run_date_column(self):
        """
        The method renames the column 'Run Date' to 'Date'
        """
        self.df = self.df.rename({'Run Date': 'Date'}, axis=1)

    def create_df_from_csv(self):
        """
        This method creates a DataFrame obj

        :return: returns a DataFrame obj
        """
        return pd.read_csv(self.infile)

    def create_trade_description_format(self, underlying):
        """
        This method will convert option trades in Fidelity's format to custom format ex. AMZN Jan01'20 3200C

        :param underlying:
        :return: trade_description has
        """
        if self.is_option(underlying):
            # Option, pull out pieces of trade
            contract_info = self.get_option_contract_info(underlying)
            ticker = contract_info.group(1)

            # Extract date format from option contracts
            contract_date = self.get_contract_date_info(contract_info)
            year_t = '20{}{}'.format(contract_date[0], contract_date[1])
            month_t = '{}{}'.format(contract_date[2], contract_date[3])
            day_t = '{}{}'.format(contract_date[4], contract_date[5])
            date_t = datetime.date(int(year_t), int(month_t), int(day_t))
            option_series = date_t.strftime("%b%d%y")

            # Option contract detail
            option_type = contract_info.group(3)
            strike_price = contract_info.group(4)
            trade_description = '{} {} {}{}'.format(ticker, option_series, strike_price, option_type)
        else:
            ## Stock
            trade_description = underlying
        return trade_description

    def create_trade_series_format(self, underlying):
        """
        This method configures output to trade_series format
        :param underlying:
        :return:  trade_series
        """
        if self.is_option(underlying):
            # Option, pull out pieces of trade
            contract_info = self.get_option_contract_info(underlying)

            # Extract date format from option contracts
            contract_date = self.get_contract_date_info(contract_info)
            year_t = '20{}{}'.format(contract_date[0], contract_date[1])
            month_t = '{}{}'.format(contract_date[2], contract_date[3])
            day_t = '{}{}'.format(contract_date[4], contract_date[5])
            date_t = datetime.date(int(year_t), int(month_t), int(day_t))
            option_series = date_t.strftime('%B %d, %Y')
            trade_series = '{}'.format(option_series)
        else:
            ## Stock
            trade_series = ''
        return trade_series

    def create_trade_type_column(self, trade_type='Long'):
        """
        Creates a new column in the Dataframe for trade type default to Long

        :param trade_type: Long or Short
        """
        self.df['Trade_Type'] = trade_type

    def create_underlying_series_trade_description_columns(self):
        """

        :return:
        """
        indexes_to_remove = []
        # Check dataset using Symbol column.  Look for rows that are not Stocks or Options save their index
        for i, sym in enumerate(self.df['Symbol']):
            if not self.is_valid_security_type(sym):
                indexes_to_remove.append(i)

        # Remove rows using indexes_to_remove list
        self.df = self.df.drop(index=indexes_to_remove, axis=0)
        # Reset the index range now that we had removed rows
        self.df.index = range(len(self.df))
        # Rename Symbol column to Underlying
        self.df = self.df.rename({'Symbol': 'Underlying'}, axis=1)
        self.df['Trade_Description'] = np.vectorize(self.create_trade_description_format)(self.df['Underlying'])
        self.df['Series'] = np.vectorize(self.create_trade_series_format)(self.df['Underlying'])
        self.df['Underlying'] = np.vectorize(self.parse_underlying)(self.df['Underlying'])

    def get_contract_date_info(self, trade_info):
        contract_date_info = trade_info.group(2)
        date_info = ''
        if len(contract_date_info) > 6:
            for i in range(len(contract_date_info)):
                if not i == 0:
                    date_info = date_info + contract_date_info[i]
        if date_info:
            contract_date_info = date_info
        return contract_date_info

    def get_option_contract_info(self, underlying):
        pattern = re.compile(r' -(\D{1,5})(\d{6,7})(\D)([0-9.]+)')
        trade_info = re.search(pattern, underlying)
        return trade_info

    def is_option(self, security):
        """
        This method will return a Boolean if security is an option or not

        :param security:
        :return: True or False
        """
        option_pattern = re.compile(r'(-)\w*')
        if re.search(option_pattern, security):
            return True
        return False

    def is_valid_security_type(self, security):
        """
        This method is used find valid security in the dataset.  If it's not an option or stock return False

        :param security:
        :return: True or False
        """
        if self.is_option(security):
            # print("This is an option = {}".format(security))
            return True
        else:
            if self.is_valid_stock(security):
                # print("STOCK!!! = {}".format(security))
                return True
            else:
                return False

    def is_valid_stock(self, security):
        """
        This method checks if the security is a valid stock.

        :param security:
        :return: True or False
        """
        pattern = re.compile(r'(\d)+')
        if re.search(pattern, security):
            return False
        return True

    def parse_underlying(self, underlying):
        """
        This method pulls out the ticker information from
        :param underlying:
        :return:
        """
        if self.is_option(underlying):
            contract_info = self.get_option_contract_info(underlying)
            ticker = contract_info.group(1)
        else:
            ticker = underlying
        return ticker

    def remove_unused_columns(self):
        """
        This method removes unused data from csv file
        """
        self.df = self.df.drop('Security Type', axis=1)
        self.df = self.df.drop('Commission ($)', axis=1)
        self.df = self.df.drop('Fees ($)', axis=1)
        self.df = self.df.drop('Accrued Interest ($)', axis=1)
        self.df = self.df.drop('Settlement Date', axis=1)
        self.df = self.df.drop('Security Description', axis=1)

    def rename_avg_price(self):
        """
        This method changes to avg_price used in final format
        """
        self.df = self.df.rename({'Price ($)': 'Avg_Price'}, axis=1)

    def rename_net_cost(self):
        """
        This method changes to Net_Cost/Credit used in final format
        :return:
        """
        self.df = self.df.rename({'Amount ($)': 'Net_Cost/Credit'}, axis=1)

    def reorder_columns(self):
        """
        This method reorders a panda dataframe into final csv format
        """
        self.df = self.df[['Date', 'Trade_Type', 'Action', 'Underlying',
                           'Series', 'Trade_Description', 'Quantity', 'Avg_Price',
                           'Net_Cost/Credit']]


def main(infile):
    """
    The is the main function
    :param infile:
    :param outfile:
    :return:  None
    """
    tl = TradeLog(infile)


if __name__ == '__main__':
    FILE_IN = "csvs_for_testing_only/rollover_2020.csv"  # input file
    #FILE_IN = "csvs_for_testing_only/natrollover_2020.csv"  # input file
    main(FILE_IN)
