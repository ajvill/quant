DROP TABLE master_watchlist2;

CREATE TABLE master_watchlist2 (
      id SERIAL,
      ticker VARCHAR(128) UNIQUE NOT NULL,
      name VARCHAR(128) UNIQUE NOT NULL,
      sector VARCHAR(128) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY(id)
);

-- Btree index
DROP INDEX mw2_unique;
CREATE INDEX mw2_unique ON master_watchlist2(ticker);
SELECT pg_relation_size('master_watchlist2'), pg_indexes_size('master_watchlist');

-- Btree md5 index
DROP INDEX mw2_md5;
CREATE INDEX mw2_md5 ON master_watchlist2(md5(ticker));
SELECT pg_relation_size('master_watchlist2'), pg_indexes_size('master_watchlist');
-- Example need to change the md5(entry) part
EXPLAIN ANALYZE SELECT ticker FROM master_watchlist2
WHERE md5(ticker) = md5('EQOTIJCNPN');


-- Example how to generate a 10 char random integer in Ascii int range 65-90 the converted A-Z in SQL
--SELECT  chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER) ) ||
--        chr( CAST( (floor(random()*(90-65+1))+65) AS INTEGER)) ;