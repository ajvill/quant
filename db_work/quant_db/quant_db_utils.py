from db_work.quant_db import hidden
import psycopg2


CREATE_WATCHLIST_MEMBERS_TABLE = """CREATE TABLE IF NOT EXISTS watchlist_members (
    id SERIAL,
    name VARCHAR(128) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
 );"""
INSERT_WATCHLIST_MEMBERS_TABLE = "INSERT INTO watchlist_members (name) VALUES ('%s');"

CREATE_MW_WATCHLIST_TABLE = """CREATE TABLE IF NOT EXISTS master_watchlist (
    id SERIAL,
    ticker VARCHAR(128) NOT NULL,
    name VARCHAR(128) UNIQUE NOT NULL,
    sector VARCHAR(128) NOT NULL,
    watchlist_members_id INTEGER REFERENCES  watchlist_members(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
);"""
INSERT_MASTER_WATCHLIST_TABLE = """INSERT INTO master_watchlist
    (ticker, name, sector)
    VALUES ('%s', '%s', '%s');"""

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
    VALUES (%d, %d, '%s', %d, '%s', %f, '%s');"""

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

quant_db_stmt_dict = {
    'watchlist_members': {
        'create': CREATE_WATCHLIST_MEMBERS_TABLE,
        'insert': INSERT_WATCHLIST_MEMBERS_TABLE
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
    }
}


def create_index(cur, index):
    sql = quant_db_stmt_dict[index]['create']
    cur.execute(sql)


def create_table(cur, table):
    sql = quant_db_stmt_dict[table]['create']
    cur.execute(sql)


def drop_index(cur, index):
    sql = "DROP INDEX IF EXISTS %s;" % (index, )
    cur.execute(sql)


def drop_table(cur, table):
    sql = "DROP TABLE IF EXISTS %s CASCADE;" % (table, )
    cur.execute(sql)


def insert_table(cur, table, params):
    col_values_list = []

    # build up values list then convert to tuple
    for key in params.keys():
        col_values_list.append(params[key])

    col_values_tuple = tuple(col_values_list)
    sql = quant_db_stmt_dict[table]['insert'] % col_values_tuple
    cur.execute(sql)


def queryValue(cur, sql, fields=None, error=None) :
    row = queryRow(cur, sql, fields, error);
    if row is None:
        return None
    return row[0]


def queryRow(cur, sql, fields=None, error=None) :
    row = doQuery(cur, sql, fields)
    try:
        row = cur.fetchone()
        return row
    except Exception as e:
        if error: 
            print(error, e)
        else :
            print(e)
        return None


def doQuery(cur, sql, fields=None) :
    row = cur.execute(sql, fields)
    return row


def get_conn():
    secrets = hidden.secrets()
    connection = psycopg2.connect(host=secrets['host'], port=secrets['port'],
                                  database=secrets['database'],
                                  user=secrets['user'],
                                  password=secrets['pass'],
                                  connect_timeout=10)
    return connection
