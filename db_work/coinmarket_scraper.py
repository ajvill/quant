import time
from datetime import datetime
import json
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import psycopg2
from sqlalchemy import create_engine
from cryptography.fernet import Fernet
import pandas as pd


def get_api_key_info():
    """
    API_KEY
    :return: api key
    """
    api_crypto_key_file = 'accounts/coinmarktcap_api.txt'
    with open(api_crypto_key_file, 'r') as file_api:
        for line in file_api:
            # assign to list but remove newline ascii at end
            key = line
    file_api.close()
    return key


def get_db_secret():
    """
    prepare dB secret for connection
    :return: plain text
    """
    with open('accounts/postgres_key.bin', 'rb') as key_object:
        for line in key_object:
            encryptedkey = line
    with open('accounts/postgres_cipheredpwd.bin', 'rb') as pwd_object:
        for line in pwd_object:
            encryptedpwd = line
    cipher_suite = Fernet(encryptedkey)
    # decrypt and convert to string
    unciphered_text = cipher_suite.decrypt(encryptedpwd)
    plain_text_decryptedpwd = bytes(unciphered_text).decode('utf-8')  # convert to string
    key_object.close()
    pwd_object.close()
    return plain_text_decryptedpwd


def prepare_db_statement(df):
    """
    Builds INSERT SQL statement into coinmarket dB.  Some preprocessing done on df before insert
    :param df:  A Series type, a row from data_df
    :return: sql_stmt
    """
    # some preprocessing on df
    # df['tags'] and df['quote'] are both lists so cast as a string
    platform_value = str(df['platform']).replace("'", "''")
    tag_value = str(df['tags']).replace("'", "''")
    quote_value = str(df['quote']).replace("'", "''")

    sql_stmt = """\
        INSERT INTO coinmarket(id, name, symbol, slug, num_market_pairs, date_added,
           tags, max_supply, circulating_supply, total_supply, platform,
           cmc_rank, last_updated, quote, date_inserted)
        VALUES
        ({}, '{}', '{}', '{}', {}, '{}', '{}', {}, {}, {}, '{}', {}, '{}', '{}', {})"""     \
        .format(df['id'], df['name'], df['symbol'], df['slug'], df['num_market_pairs'],     \
                df['date_added'], tag_value, df['max_supply'], df['circulating_supply'], df['total_supply'],\
                platform_value, df['cmc_rank'], df['last_updated'], quote_value, 'CURRENT_TIMESTAMP')
    return sql_stmt


def main():
    """
    MAIN
    """
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '100',
        'convert': 'USD'
    }
    api_key = get_api_key_info()
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }
    session = Session()
    session.headers.update(headers)
    # db secret
    plain_text_decryptedpwd = get_db_secret()
    db = create_engine(
        'postgresql+psycopg2://postgres:{}@localhost:5434/marketdata'.format(plain_text_decryptedpwd))
    try:
        while True:
            now = datetime.now()
            file_ext = now.strftime("%Y%m%d%H%M%S")
            response = session.get(url, params=parameters)
            data = json.loads(response.text)
            data_df = pd.DataFrame(data=data['data'])
            # some rows may have NaN so dB expects Double
            data_df['max_supply'] = data_df['max_supply'].fillna(0.0)
            num_errors = 0
            with db.connect() as conn:
                for i in range(len(data_df)):
                    sql_stmt = prepare_db_statement(data_df.iloc[i])
                    try:
                        conn.execute(sql_stmt)
                    except:
                        num_errors += 1
                        print('missed a query!!! row = {}, errors this dB insert round = {}'.format(i, num_errors))
            #data_df.to_csv('/Volumes/WorkerBee2/coinmarket_data_test/coinmarket_{}.csv'.format(file_ext))
            print('coinmarket5min inserted at {} time'.format(file_ext))
            time.sleep(300)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


if __name__ == '__main__':
    main()