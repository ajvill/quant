DROP TABLE accounts CASCADE;

CREATE TABLE accounts (
    id SERIAL,
    name VARCHAR(128) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
);

DROP TABLE master_watchlist CASCADE;

CREATE TABLE master_watchlist (
    id SERIAL,
    ticker VARCHAR(128) NOT NULL,
    name VARCHAR(128) UNIQUE NOT NULL,
    sector VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
);

DROP INDEX mw_unique;
CREATE INDEX mw_unique ON master_watchlist(ticker);
SELECT pg_relation_size('master_watchlist'), pg_indexes_size('master_watchlist');

DROP TABLE positions CASCADE;

CREATE TABLE positions (
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
);

DROP TABLE portfolio CASCADE;

CREATE TABLE portfolio (
    id SERIAL,
    total_cash NUMERIC(10, 4),
    year_starting_balance NUMERIC(10, 4),
    starting_balance NUMERIC(10, 4),
    accounts_id INTEGER REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
);

DROP TABLE trade_log CASCADE;

CREATE TABLE trade_log (
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
);

DROP TABLE daily_performance CASCADE;

CREATE TABLE daily_performance (
    id SERIAL,
    date DATE,
    daily_ending_balance NUMERIC(10, 4),
    portfolio_id INTEGER REFERENCES portfolio(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(id)
);

-- Stored Procedures
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
 RETURNS TRIGGER AS $$
 BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
 END;
 $$ LANGUAGE plpgsql;

 CREATE TRIGGER set_timestamp
 BEFORE UPDATE ON accounts
 FOR EACH ROW
 EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON master_watchlist
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON positions
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON portfolio
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON trade_log
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TRIGGER set_timestamp
BEFORE UPDATE ON daily_performance
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

-- to view triggers
SELECT  event_object_table AS table_name ,trigger_name
FROM information_schema.triggers
GROUP BY table_name , trigger_name
ORDER BY table_name ,trigger_name;

-- list functions
-- \df

-- show function name
SELECT prosrc FROM pg_proc WHERE proname = 'trigger_set_timestamp';