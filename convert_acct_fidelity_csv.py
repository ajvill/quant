# importing csv module
import csv
import operator
from collections import Counter
from collections import defaultdict
from datetime import date


def parse_action(test_string):
    """
    converts test_string  to a SLD or Bot action.
    :param test_string: The string we will examine for the action
    :return action = SLD or BOT:
    """

    action = ""
    test = test_string.split()

    # Sort between two keywords used "Bought" or "SOLD"
    if test[1] == "SOLD":
        action = "SLD"
    else:
        action = "Bot"
    return action


def parse_underlying(test_string):
    """
    This method filters out the underlying from an option name
    :param test_string:  String will test for underlying
    :return:
    """
    security = test_string
    underlying = ""
    underlying_len = 0
    is_a_stock = False
    ticker_locate = True
    ticker_count = 0
    char_count = 0

    if security.startswith('-', 1):
        # This is an option
        for element in security:
            if element.isalpha() and ticker_count < 5 and ticker_locate:
                underlying = underlying + element
                ticker_count += 1
            if element.isnumeric() and ticker_count < 5 and char_count > 0:
                ticker_locate = False
            char_count += 1
    else:
        # This is a stock
        underlying = security
        is_a_stock = True
    # remove any whitespace in front of ticker symbol
    underlying = underlying.lstrip(' ')
    underlying_len = len(underlying)
    return underlying, underlying_len, is_a_stock


def get_month_series(month):
    """
    This method converts the month from 01-12 fomat to January  and Jan used option series or option formats
    :param month:
    :return month and month contract:
    """
    month_contract = ""

    if month == "01":
        month = "January"
        month_contract = "Jan"
    elif month == "02":
        month = "February"
        month_contract = "Feb"
    elif month == "03":
        month = "March"
        month_contract = "Mar"
    elif month == "04":
        month = "April"
        month_contract = "Apr"
    elif month == "05":
        month = "May"
        month_contract = "May"
    elif month == "06":
        month = "June"
        month_contract = "Jun"
    elif month == "07":
        month = "July"
        month_contract = "Jul"
    elif month == "08":
        month = "August"
        month_contract = "Aug"
    elif month == "09":
        month = "September"
        month_contract = "Sep"
    elif month == "10":
        month = "October"
        month_contract = "Oct"
    elif month == "11":
        month = "November"
        month_contract = "Nov"
    elif month == "12":
        month = "December"
        month_contract = "Dec"
    return month, month_contract


def parse_series(test_string, underlying_len):
    """
    This method filters out the Series of the option contract
    :param test_string:
    :param underlying_len:
    :return series, month_contract, day, year, contract_type, strike_price:
    """
    security = test_string
    series = ""
    year = ""
    month = ""
    month_contract = ""
    day = ""
    contract_type = ""
    strike_price = ""

    # use these to keep track of each char in the string
    char_count = 0
    year_count = 0
    month_count = 0
    day_count = 0
    # -AMZN200821C3200 example of test_string filter out "-"
    if security.startswith('-', 1):
        # This is an option
        for element in security:
            # filter year month day (ex: 200821 in -AMZN200821C3200) begins on char_count = 6
            if underlying_len == 1:
                # example: -M200911C6
                if element.isnumeric() and 1 < char_count < 9:
                    if year_count < 2 and 3 <= char_count <= 4:
                        year = year + element
                        year_count += 1
                    if month_count < 2 and 5 <= char_count <= 6:
                        month = month + element
                        month_count += 1
                    if day_count < 2 and 7 <= char_count <= 8:
                        day = day + element
                        day_count += 1
                elif char_count >= 9 and not element.isspace():
                    if char_count == 9:
                        contract_type = element
                    else:
                        strike_price = strike_price + element
            if underlying_len == 2:
                # example: -KO200911C50
                if element.isnumeric() and 2 < char_count < 10:
                    if year_count < 2 and 4 <= char_count <= 5:
                        year = year + element
                        year_count += 1
                    if month_count < 2 and 6 <= char_count <= 7:
                        month = month + element
                        month_count += 1
                    if day_count < 2 and 8 <= char_count <= 9:
                        day = day + element
                        day_count += 1
                elif char_count >= 10 and not element.isspace():
                    if char_count == 10:
                        contract_type = element
                    else:
                        strike_price = strike_price + element
            if underlying_len == 3:
                # example: -QQQ200911C268
                if element.isnumeric() and 3 < char_count < 11:
                    if year_count < 2 and 5 <= char_count <= 6:
                        year = year + element
                        year_count += 1
                    if month_count < 2 and 7 <= char_count <= 8:
                        month = month + element
                        month_count += 1
                    if day_count < 2 and 9 <= char_count <= 10:
                        day = day + element
                        day_count += 1
                elif char_count >= 11 and not element.isspace():
                    if char_count == 11:
                        contract_type = element
                    else:
                        strike_price = strike_price + element
            if underlying_len == 4:
                # example: -AMZN200821C3200
                if element.isnumeric() and 5 < char_count < 12:
                    if year_count < 2 and 6 <= char_count <= 7:
                        year = year + element
                        year_count += 1
                    if month_count < 2 and 8 <= char_count <= 9:
                        month = month + element
                        month_count += 1
                    if day_count < 2 and 10 <= char_count <= 11:
                        day = day + element
                        day_count += 1
                elif char_count >= 12 and not element.isspace():
                    if char_count == 12:
                        contract_type = element
                    else:
                        strike_price = strike_price + element
            if underlying_len == 5:
                # example:  -GOOGL200911C1530
                if element.isnumeric() and 6 < char_count < 13:
                    if year_count < 2 and 7 <= char_count <= 8:
                        year = year + element
                        year_count += 1
                    if month_count < 2 and 9 <= char_count <= 10:
                        month = month + element
                        month_count += 1
                    if day_count < 2 and 11 <= char_count <= 12:
                        day = day + element
                        day_count += 1
                elif char_count >= 13 and not element.isspace():
                    if char_count == 13:
                        contract_type = element
                    else:
                        strike_price = strike_price + element
            char_count += 1

    # use helper functions to get format
    year_name = year
    month, month_contract = get_month_series(month)

    series = "%s %s, 20%s" % (month, day, year)
    return series, month_contract, day, year, contract_type, strike_price




def check_already_tested(already_tested, ticker):
    """
    This method checks if we tested this ticker and all of it's transactions
    we return true if we have so we don't and move on
    :param already_tested:
    :param ticker:
    :return new_sort:
    """
    new_sort = False
    for ticker_tmp in already_tested:
        if ticker_tmp == ticker:
            new_sort = True
    return new_sort


def is_stock(series):
    """
    Is this a stock or option
    :param series:
    :return stock:
    """
    stock = False
    if series == "":
        stock = True
    return stock


def option_calculator(option_list_sorted, ticker, num_total_transactions):
    """
    This method will look at transaction for an option look for complete transactions and write result
    to a new list
    :param option_list_sorted:
    :param ticker:
    :param num_total_transactions:
    :return option_tradelog_list:
    """
    option_tradelog_list = []
    index = 0
    complete_transactions_found = 0

    # use these for tracking transactions
    option_bot_debit = 0.0
    option_sld_credit = 0.0
    option_bot_quantity = 0
    option_sld_quantity = 0
    found_bot_transaction = False
    found_sld_transaction = False

    for col in option_list_sorted:
        #print("****{},  num_total_transactions = {}".format(option_trans, num_total_transactions))
        transaction_date = col[0]  # Open or Closed date
        trade_type = col[1]
        action = col[2]  # Bot or SLD
        underlying = col[3]
        contract_series = col[4]
        quantity = col[6]  # number of lots
        cost = col[8]  # cost of transaction either when Bot or SLD

        if index < num_total_transactions:
            # Start by checking if we have a valid Bot or SLD don't want first entry for an underlying to be SLD
            # because that implies a previous timeframe that is not with this data.  This is normal Bot and SLD
            if not (action == "SLD" and index == 0) or (action == "Bot" and quantity == 0):
                print("underlying = {}, action = {}, quantity = {}".format(underlying, action, quantity))
                if action == "Bot":
                    option_open_trans_date = [x for x in transaction_date.split("/")]
                    option_bot_quantity = int(option_bot_quantity) + int(quantity)
                    option_bot_debit = float(option_bot_debit) + float(cost)
                    found_bot_transaction = True
                if action == "SLD":
                    option_closed_trans_date = [x for x in transaction_date.split("/")]
                    option_sld_quantity = int(option_sld_quantity) + int(quantity)
                    option_sld_credit = float(option_sld_credit) + float(cost)
                    found_sld_transaction = True
            else:
                print("A throw away!!!---->{}".format(col))
        index += 1
    print("Found %d completed option transactions out of %d total option transactions when evaluating %s" % (complete_transactions_found,
                                                                                                num_total_transactions, ticker))
    return option_tradelog_list


def stock_calculator(stock_list_sorted, ticker, num_total_transactions):
    """
    This method will look at transaction for a stock look for complete transactions and write result
    to a new list
    :param stock_list_sorted:
    :param ticker:
    :param num_total_transactions:
    :return stock_tradelog_list:
    """
    stock_tradelog_list_tmp = []  # tradelog list to return
    stock_tradelog_list = []
    index = 0
    complete_transactions_found = 0

    # use these for tracking transactions
    stock_bot_debit = 0.0
    stock_sld_credit = 0.0
    stock_bot_quantity = 0
    stock_sld_quantity = 0
    found_bot_transaction = False
    found_sld_transaction = False

    # go through list identify open transactions and make list
    for col in stock_list_sorted:
        # This first check is for stocks bought in previous month
        transaction_date = str(col[0])  # Open or Closed date
        trade_type = col[1]
        action = col[2]  # Bot or SLD
        underlying = col[3]
        quantity = col[6]  # number of lots
        cost = col[8]  # cost of transaction either when Bot or SLD
        trade_outcome = " "  # W or L

        # TODO recalculate avg price
        if index < num_total_transactions:
            if not (action == "SLD" and index == 0) or (action == "Bot" and quantity == 0):
                # Normal case, index = 0 wil normally be a Bot transaction followed by either a Bot or SLD
                if action == "Bot":
                    stock_open_trans_date = [x for x in transaction_date.split("/")]
                    stock_bot_quantity = int(stock_bot_quantity) + int(quantity)
                    stock_bot_debit = float(stock_bot_debit) + float(cost)
                    found_bot_transaction = True
                if action == "SLD":
                    stock_closed_trans_date = [x for x in transaction_date.split("/")]
                    stock_sld_quantity = int(stock_sld_quantity) + int(quantity)
                    stock_sld_credit = float(stock_sld_credit) + float(cost)
                    found_sld_transaction = True
            # Here looking for SLD transaction to identify a complete_transaction_found
            if action == "SLD" and not stock_bot_quantity == 0 and not stock_sld_quantity == 0:
                # Normal case buy stock and then sell all stock closing full transaction
                if int(stock_bot_quantity) + int(stock_sld_quantity) == 0:
                    # completed transaction so log info
                    complete_transactions_found += 1

                    # determine if transaction was a W or L
                    if float(stock_sld_credit) >= abs(float(stock_bot_debit)):
                        trade_outcome = "W"
                    else:
                        trade_outcome = "L"

                    # calculate realized gain
                    realized_gain = str(float(stock_sld_credit) - abs(float(stock_bot_debit)))

                    if float(stock_bot_debit) < 0:
                        realized_gain_percent = str(float(realized_gain)/abs(float(stock_bot_debit)))
                    else:
                        realized_gain_percent = " "

                    # Calculate the days hold
                    open_date = date(int(stock_open_trans_date[2]),
                                     int(stock_open_trans_date[0]),
                                     int(stock_open_trans_date[1]))

                    closed_date = date(int(stock_closed_trans_date[2]),
                                    int(stock_closed_trans_date[0]),
                                     int(stock_closed_trans_date[1]))
                    days_held = closed_date - open_date

                    # Create tradelog list in CSV format
                    stock_tradelog_list_tmp = [transaction_date, trade_type, "closed", underlying, " ", underlying, str(stock_bot_quantity), " ",
                                          "$%s" % str(round(abs(stock_bot_debit), 2)), "$%s" % str(round(stock_sld_credit, 2)), transaction_date, trade_outcome,
                                          realized_gain, realized_gain_percent, str(days_held.days)]
                    stock_tradelog_list.append(stock_tradelog_list_tmp)

                # Case when 1st Bot quantity is less than SLD quantity
                elif stock_bot_quantity < abs(stock_sld_quantity):
                    # Do not add to list but maybe create separate log
                    print("Found a transaction where stock Bot quantity is less than SLD quantity "
                          "don't count transaction, ticker = {}".format(ticker))
        index += 1
    # End of for loop
    # This case is when position is open because we Bot once or more times with any SLD
    if not found_sld_transaction and not stock_bot_quantity == 0:
        # Add to list
        print("Case where we still have an open position, make list...we only added to ticker: %s" % (ticker))
    # This case is when we Bot shares and SLD some shares but position is still open
    elif found_bot_transaction and found_sld_transaction and stock_bot_quantity > stock_sld_quantity and not complete_transactions_found:
        print("Case when we Bot stock and SLD some but position still open, ticker {}".format(ticker))
    #print("Found %d stock transactions out of %d total stock transactions when evaluating %s" % (complete_transactions_found,
     #                                                                                            num_total_transactions, ticker))
    return stock_tradelog_list


def tradelog_calculator(ticker_log_transactions, ticker, tradelog_list_ticker_count, already_tested, num_stock_transactions, num_options_transactions):
    """
    The method will split transactions into stock or option depending on transaction type.
    :param ticker_log_transactions:
    :param ticker:
    :param tradelog_list_ticker_count:
    :param already_tested:
    :param num_stock_transactions:
    :param num_options_transactions:
    :return final_tradelog_list, already_tested, num_stock_transactions, num_options_transactions:
    """
    # final tradelog in a csv file format
    final_tradelog_list = []
    stock_list = []
    option_list = []
    num_total_transactions = tradelog_list_ticker_count[ticker]
    transactions_found = 0

    # check if we're dealing with stock or option transaction split them into separate lists
    for col in ticker_log_transactions[:]:
        if is_stock(col[4]):
            stock_list.append(col)
            num_stock_transactions += 1
        else:
            option_list.append(col)
            num_options_transactions += 1
    # sort stock_list by date
    stock_list_sorted = sorted(stock_list, key=operator.itemgetter(0))
    final_stock_list = (stock_calculator(stock_list_sorted, ticker, num_total_transactions))

    option_list_sorted = sorted(option_list, key=operator.itemgetter(0))
    final_option_list = (option_calculator(option_list_sorted, ticker, num_total_transactions))
    #final_option_list = (stock_calculator(option_list_sorted, ticker, num_total_transactions))

    # update already_tested with completed symbol so we move to next ticker not repeat this set
    already_tested.add(ticker)
    #final_tradelog_list  = final_stock_list
    final_tradelog_list  = final_stock_list + final_option_list
    return final_tradelog_list, already_tested, num_stock_transactions, num_options_transactions


def get_log_transactions(index, ticker, tradelog_sorted):
    """
    The method gets the total number of transactions for a ticker
    :param index:
    :param ticker:
    :param tradelog_sorted:
    :return ticker_log_transactions:
    """
    ticker_log_transactions = []
    for col in tradelog_sorted[index:]:
        if ticker == col[3]:
            # add row to list
            ticker_log_transactions.append(col)
    return ticker_log_transactions


def create_final_csv_format(tradelog_sorted, tradelog_list_ticker_count):
    """
    The method creates the final csv file that will be the completed list
    :param tradelog_sorted:
    :param tradelog_list_ticker_count:
    :return final_tradelog_list:
    """
    final_tradelog_list = []
    final_tradelog_list_tmp = []
    index = 0
    already_tested = set()
    num_stock_transactions = 0
    num_options_transactions = 0
    for col in tradelog_sorted[:]:
        # Empty list for use inside this loop
        ticker_log_transactions = []
        ticker = str(col[3])
        # Filter out non-trade inputs
        if ticker != "SPAXX":
            first_char_of_ticker = str(ticker[0])
            if first_char_of_ticker.isalpha():
                if not check_already_tested(already_tested, ticker):
                    # In here is where the magic is...need to count trade transactions per ticker
                    ticker_log_transactions = get_log_transactions(index, ticker, tradelog_sorted)
                    final_tradelog_list_tmp, already_tested, num_stock_transactions, num_options_transactions = tradelog_calculator(ticker_log_transactions, ticker, tradelog_list_ticker_count,
                                                                              already_tested, num_stock_transactions, num_options_transactions)
                    final_tradelog_list += final_tradelog_list_tmp
        index += 1
    print("There are {} total stock and {} total option transactions".format(num_stock_transactions, num_options_transactions))
    return final_tradelog_list


def create_initial_csv_format(csv_file_contents, initial_tradelog_list):
    """
    This method takes in csv data from Fidelity and converts into a new csv list using performance format

    :param csv_file_contents: object containing original Fidelity csv information
    :param initial_tradelog_list: empty list containing desired number of columns to convert csv_file_contents
    :return initial_tradelog_list: first pass conversion list containing all transactions dumped here
    """
    i, j = 0, 0
    new_entry = False

    # go through every row in csv file
    for col in csv_file_contents[:]:
        # parsing each column of a row
        for char in col:
            # underlying_len is used to determine location of year month date
            underlying_len = 0
            month_contract = ""
            day = ""
            year = ""
            contract_type = ""
            strike_price = ""
            is_a_stock = False

            if col[0] and not new_entry:
                # Date
                initial_tradelog_list[i][j] = col[0]
            if col[1] and not new_entry:
                # Trade Type and Action
                # TODO "Trade Type" is being defaulted to Long but need to add Long and Short
                initial_tradelog_list[i][j + 1] = "Long"
                # Will return Action log keyword Bot or SLD
                initial_tradelog_list[i][j + 2] = str(parse_action("%s" % col[1]))
            if col[2] and not new_entry:
                # Underlying goes here
                initial_tradelog_list[i][j + 3], underlying_len, is_a_stock = parse_underlying("%s" % col[2])
                # Series
                # NOTE don't call parse_series if a stock, only for option trades
                if not is_a_stock:
                    initial_tradelog_list[i][j + 4], month_contract, day, year, contract_type, strike_price = parse_series("%s" % col[2], underlying_len)
                else:
                    initial_tradelog_list[i][j + 4] = ""
            if col[3] and not new_entry:
                # Trade Description
                initial_tradelog_list[i][j + 5] = "%s%s'%s %s%s" % (month_contract, day, year, strike_price, contract_type)
            if col[5] and not new_entry:
                # Quantity
                initial_tradelog_list[i][j + 6] = "%s" % col[5]
            if col[6] and not new_entry:
                # Avg Price
                initial_tradelog_list[i][j + 7] = "%s" % col[6]
            if col[10] and not new_entry:
                # Net Cost/Credit
                initial_tradelog_list[i][j + 8] = "%s" % col[10]
            new_entry = True
            j += 1
        new_entry = False
        j = 0
        i += 1
    return initial_tradelog_list


# MAIN
# initializing the titles and rows list
def main(infile, outfile):
    fields = []
    csv_file_contents = []

    # reading csv file
    with open(infile, encoding='utf-8') as csvfile_read:
        # creating a csv reader object
        csvreader = csv.reader(csvfile_read)

        # extracting field names through first row
        fields = next(csvreader)

        # extracting each data row one by one
        for row in csvreader:
            csv_file_contents.append(row)
    csvfile_read.close()

    # create new list for csv output file
    num_columns = 9
    # rows
    num_rows = csvreader.line_num
    initial_tradelog_list = [[0 for x in range(num_columns)] for y in range(num_rows-1)]

    # This is where we sort through the csv file and place it in a new list to output to a new csv file
    initial_tradelog_list = create_initial_csv_format(csv_file_contents, initial_tradelog_list)

    # sorts 1st by option contract name if exists, 2nd by underlying ticker symbol, 3rd if Bot or SLD
    tradelog_sorted = sorted(initial_tradelog_list, key=operator.itemgetter(5, 3, 2, 0))

    # don't need intial_tradelog_list anymore now that we have tradelog_sorted
    del initial_tradelog_list

    # Filter out the tickers from the trade history list in new_sorted_list into condensed list showing only the tickers
    tickers = []
    for col in tradelog_sorted[:]:
        if str(col[3]) != "SPAXX":
            first_char_ticker = str(col[3][0])
            if first_char_ticker.isalpha():
                tickers.append("%s" % col[3])

    # Initialize a dict type to default value of 0 using lambda function
    tradelog_list_ticker_count = defaultdict(lambda: 0)
    # using collections get the number of transactions per ticker symbol, then convert to a dict type
    tradelog_list_ticker_count.update(dict(Counter(tickers)))
    final_tradelog_list_tmp = create_final_csv_format(tradelog_sorted, tradelog_list_ticker_count)
    final_tradelog_list = sorted(final_tradelog_list_tmp, key=operator.itemgetter(0))

    # writing to csv file
    with open(outfile, 'w') as csvfile_write:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile_write)

        # writing the data rows
        csvwriter.writerows(final_tradelog_list)
    csvfile_write.close()


if __name__ == '__main__':
    # infile = "testAccount.csv"  # input file
    #infile = "testAccount2.csv"  # input file
    # infile = "nat_rollover_30days.csv"  # input file
    infile = "nat_rollover_90days.csv"  # input file
    outfile = "test_out.csv"  # output file
    main(infile, outfile)