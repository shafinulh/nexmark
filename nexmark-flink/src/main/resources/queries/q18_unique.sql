-- -------------------------------------------------------------------------------------------------
-- Query 18 Unique: Find last bid (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- What's a's last bid for bidder to auction?
-- Illustrates a Deduplicate query while reading from the unique source tables.
-- -------------------------------------------------------------------------------------------------

CREATE TABLE nexmark_q18 (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    `dateTime`  TIMESTAMP(3),
    extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q18
SELECT auction, bidder, price, channel, url, `dateTime`, extra
 FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY `dateTime` DESC) AS rank_number
       FROM bid)
 WHERE rank_number <= 1;
