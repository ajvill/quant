from src.quant_db import hidden
import pandas as pd
import os
import re
import psycopg2
import logging

logger = logging.getLogger()


class QuantDB:
    QUANT_TABLES = ('watchlist_members', 'master_watchlist', 'accounts', 'positions',
                    'portfolio', 'trade_log', 'daily_performance')

    QUANT_INDEXES = ('wl_unique', 'mw_unique')

    CREATE_WATCHLIST_MEMBERS_TABLE = """CREATE TABLE IF NOT EXISTS watchlist_members (
        id SERIAL,
        name VARCHAR(128) UNIQUE NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
     );"""
    INSERT_WATCHLIST_MEMBERS_TABLE = "INSERT INTO watchlist_members (name) VALUES ('%s');"
    SELECT_WATCHLIST_MEMBERS_LIST = "SELECT id, name FROM watchlist_members;"
    SELECT_WATCHLIST_MEMBERS_FULL_LIST = "SELECT * FROM watchlist_members;"

    CREATE_MW_WATCHLIST_TABLE = """CREATE TABLE IF NOT EXISTS master_watchlist (
        id SERIAL,
        ticker VARCHAR(128) UNIQUE NOT NULL,
        name VARCHAR(128) UNIQUE NOT NULL,
        sector VARCHAR(128) NOT NULL,
        exchange VARCHAR(128) NOT NULL,
        --watchlist_members_id INTEGER REFERENCES  watchlist_members(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_MASTER_WATCHLIST_TABLE = """INSERT INTO master_watchlist
        (ticker, name, sector, exchange) VALUES ('%s', '%s', '%s', '%s');"""

    CREATE_ACCOUNTS_TABLE = """CREATE TABLE IF NOT EXISTS accounts (
        id SERIAL,
        name VARCHAR(128) UNIQUE NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_ACCOUNTS_TABLE = "INSERT INTO accounts (name) VALUES ('%s');"

    CREATE_POSITIONS_TABLE = """CREATE TABLE IF NOT EXISTS positions (
        id SERIAL,
        option INTEGER,
        ticker_id INTEGER REFERENCES master_watchlist(id) ON DELETE CASCADE,
        position CHAR(5) UNIQUE NOT NULL,
        quantity SMALLINT,
        open_date DATE,
        cost_basis numeric(10, 4),
        accounts_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_POSITIONS_TABLE = """INSERT INTO positions
        (option, ticker_id, position, quantity, open_date, cost_basis, accounts_id)
        VALUES (%d, %d, '%s', %d, '%s', %f, %d);"""

    CREATE_PORTFOLIO_TABLE = """CREATE TABLE IF NOT EXISTS portfolio (
        id SERIAL,
        total_cash NUMERIC(10, 4),
        year_starting_balance NUMERIC(10, 4),
        starting_balance NUMERIC(10, 4),
        accounts_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_PORTFOLIO_TABLE = """INSERT INTO portfolio
        (total_cash, year_starting_balance, starting_balance, accounts_id)
        VALUES (%f, %f, %f, %d);"""

    CREATE_TRADE_LOG_TABLE = """CREATE TABLE IF NOT EXISTS trade_log (
        id SERIAL,
        date DATE,
        trade_type CHAR(5),
        action CHAR(4),
        ticker_id INTEGER REFERENCES master_watchlist(id) ON DELETE CASCADE,
        series VARCHAR(128),
        trade_description TEXT,
        quantity SMALLINT,
        avg_price NUMERIC(10, 4),
        cost_credit NUMERIC(10, 4),
        exit_price NUMERIC(10, 4),
        exit_date DATE,
        positions_id INTEGER REFERENCES positions(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_TRADE_LOG_TABLE = """INSERT INTO trade_log
        (date, trade_type, action, ticker_id, series, trade_description, quantity, avg_price,
        cost_credit, exit_date, positions_id)
        VALUES ('%s', '%s', '%s', %d, '%s', '%s', %d, %f, %f, %f, '%s', %d);"""

    CREATE_DAILY_PERFORMANCE_TABLE = """CREATE TABLE IF NOT EXISTS daily_performance (
        id SERIAL,
        date DATE,
        daily_ending_balance NUMERIC(10, 4),
        portfolio_id INTEGER REFERENCES portfolio(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(id)
    );"""
    INSERT_DAILY_PERFORMANCE_TABLE = """INSERT INTO daily_performance
        (date, daily_ending_balance, portfolio_id)
        VALUES ('%s', %f, %d);"""

    CREATE_WL_UNIQUE_INDEX = "CREATE INDEX IF NOT EXISTS  wl_unique ON watchlist_members(name);"
    CREATE_MW_UNIQUE_INDEX = "CREATE INDEX IF NOT EXISTS  mw_unique ON master_watchlist(ticker);"

    SELECT_TABLE_INDEX_COLUMN_LIST = """SELECT 
        t.relname AS table_name, i.relname AS index_name, a.attname AS column_name
        FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
        WHERE
            t.oid = ix.indrelid
            AND i.oid = ix.indexrelid
            AND a.attrelid = t.oid
            AND a.attnum = ANY(ix.indkey)
            AND t.relkind = 'r'
            -- and t.relname like 'mytable'
        ORDER BY t.relname, i.relname;"""


    CREATE_FUNCTION_TRIGGER_SET_TIMESTAMP = """CREATE OR REPLACE FUNCTION trigger_set_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;"""

    CREATE_TRIGGER_STORED_PROCEDURE = """CREATE TRIGGER set_timestamp
        BEFORE UPDATE ON %s
        FOR EACH ROW
        EXECUTE PROCEDURE trigger_set_timestamp();"""

    quant_db_stmt_dict = {
        'watchlist_members': {
            'create': CREATE_WATCHLIST_MEMBERS_TABLE,
            'insert': INSERT_WATCHLIST_MEMBERS_TABLE,
            'select': SELECT_WATCHLIST_MEMBERS_LIST,
            'select_full': SELECT_WATCHLIST_MEMBERS_LIST
        },
        'master_watchlist': {
            'create': CREATE_MW_WATCHLIST_TABLE,
            'insert': INSERT_MASTER_WATCHLIST_TABLE
        },
        'accounts': {
            'create': CREATE_ACCOUNTS_TABLE,
            'insert': INSERT_ACCOUNTS_TABLE
        },
        'positions': {
            'create': CREATE_POSITIONS_TABLE,
            'insert': INSERT_POSITIONS_TABLE
        },
        'portfolio': {
            'create': CREATE_PORTFOLIO_TABLE,
            'insert': INSERT_PORTFOLIO_TABLE
        },
        'trade_log': {
            'create': CREATE_TRADE_LOG_TABLE,
            'insert': INSERT_TRADE_LOG_TABLE
        },
        'daily_performance': {
            'create': CREATE_DAILY_PERFORMANCE_TABLE,
            'insert': INSERT_DAILY_PERFORMANCE_TABLE
        },
        'wl_unique': {
            'create': CREATE_WL_UNIQUE_INDEX
        },
        'mw_unique': {
            'create': CREATE_MW_UNIQUE_INDEX
        },
        'table_index_column_list': SELECT_TABLE_INDEX_COLUMN_LIST,
        'trigger_set_timestamp': CREATE_FUNCTION_TRIGGER_SET_TIMESTAMP,
        'create_trigger': CREATE_TRIGGER_STORED_PROCEDURE
    }

    def __init__(self, db_test=False):
        self.conn = self.get_conn(db_test)

    def create_col_values_tuple(self, params):
        col_values_list = []

        for key in params.keys():
            col_values_list.append(params[key])

        return tuple(col_values_list)

    def create_function_trigger(self, cur):
        sql = self.quant_db_stmt_dict['trigger_set_timestamp']
        try:
            cur.execute(sql)
            logger.info("Creating function trigger_set_timestamp()")
        except (Exception, psycopg2.Error) as error:
            logger.error("Creating function failed {} error: {}".format(sql, error))
            return error.pgerror

        return None

    def create_index(self, cur, index):
        sql = self.quant_db_stmt_dict[index]['create']
        try:
            cur.execute(sql)
            logger.info("Created index {}".format(index))
        except (Exception, psycopg2.Error) as error:
            logger.error("Create index failed for {} error: {}".format(index, error))
            return error.pgerror

        return None

    def create_table(self, cur, table):
        sql = self.quant_db_stmt_dict[table]['create']
        try:
            cur.execute(sql)
            logger.info("Created table {}".format(table))
        except (Exception, psycopg2.Error) as error:
            logger.error("Create table failed for {} error: {}".format(table, error))
            return error.pgerror

        return None

    def create_trigger_stored_procedure(self, cur, table):
        sql = self.quant_db_stmt_dict['create_trigger']

        table_list = []
        table_list.append(table)
        col_sql_trigger_stmt = sql % tuple(table_list)
        try:
            cur.execute(col_sql_trigger_stmt)
            logger.info("Created triggered stored procedure for table: {}".format(table))
        except (Exception, psycopg2.Error) as error:
            logger.error("Creating triggered stored procedure for {} failed, error: {}"
                         .format(table, error))
            return error.pgerror

        return None

    def create_watchlist_table(self, cur, table_name):
        sql = """CREATE TABLE IF NOT EXISTS %s (
            id SERIAL,
            ticker_id INTEGER REFERENCES master_watchlist(id) ON DELETE CASCADE,
            wl_id INTEGER REFERENCES watchlist_members(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(id)
         );""" % (table_name, )
        try:
            cur.execute(sql)
            logger.info("Created table {}".format(table_name))
        except (Exception, psycopg2.Error) as error:
            logger.error("Create table failed for {} error: {}".format(table_name, error))
            return error.pgerror

        return None

    def drop_index(self, cur, index):
        sql = "DROP INDEX IF EXISTS %s;" % (index, )
        try:
            cur.execute(sql)
            logger.info("Dropped index {}".format(index))
        except (Exception, psycopg2.Error) as error:
            logger.error("Drop index failed for {} error: {}".format(index, error))
            return error.pgerror

        return None

    def drop_table(self, cur, table):
        sql = "DROP TABLE IF EXISTS %s CASCADE;" % (table, )
        try:
            cur.execute(sql)
            logger.info("Dropping table {}".format(table))
        except (Exception, psycopg2.Error) as error:
            logger.error("Drop table failed for {} error: {}".format(table, error))
            return error.pgerror

        return None

    def get_watchlist_members(self, cur, table):
        try:
            cur.execute(self.quant_db_stmt_dict[table]['select'])
            logger.info("Returning contents from table {}".format(table))
        except (Exception, psycopg2.Error) as error:
            logger.error("Returning table failed for {} error: {}".format(table, error))
            return error.pgerror

        return cur.fetchall()

    def insert_table(self, cur, table, params):

        col_values_tuple = self.create_col_values_tuple(params)
        sql = self.quant_db_stmt_dict[table]['insert'] % col_values_tuple
        try:
            cur.execute(sql)
            logger.info("Inserting data into table {}".format(table))
        except (Exception, psycopg2.Error) as error:
            logger.error("Inserting data into {} failed for error: {}".format(table, error))
            return error.pgerror

        return None

    def update_table(self, cur, table, params):

        col_values_tuple = self.create_col_values_tuple(params)
        sql = self.quant_db_stmt_dict[table]['update'] % col_values_tuple
        try:
            cur.execute(sql)
            logger.info("Updating table {}, sql: {}".format(table, sql))
        except (Exception, psycopg2.Error) as error:
            logger.error("Update table {} failed, error: {}".format(table, error))
            return error.pgerror

        return None

    def queryValue(self, cur, sql, fields=None, error=None) :
        row = self.queryRow(cur, sql, fields, error);
        if row is None:
            return None
        return row[0]

    def queryRow(self, cur, sql, fields=None, error=None) :
        row = self.doQuery(cur, sql, fields)
        try:
            row = cur.fetchone()
            return row
        except Exception as e:
            if error:
                print(error, e)
            else :
                print(e)
            return None

    def doQuery(self, cur, sql, fields=None) :
        row = cur.execute(sql, fields)
        return row

    def get_conn(self, db_test):

        if db_test is False:
            secrets = hidden.secrets()
        else:
            secrets = hidden.secrets_test()

        try:
            connection = psycopg2.connect(host=secrets['host'], port=secrets['port'],
                                          database=secrets['database'],
                                          user=secrets['user'],
                                          password=secrets['pass'],
                                          connect_timeout=10)
            logger.info("Quant DB connection established")
        except (Exception, psycopg2.Error) as error:
            logger.error("Error while establishing a connection to postgres, error {}".format(error))
            return error.pgerror

        return connection
