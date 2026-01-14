-- -------------------------------------------------------------------------------------------------
-- Query 20: Expand bid with auction (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Get bids with the corresponding auction information where category is 10.
-- Includes unique identifiers for both bid and auction sides of the join.
-- Illustrates a filter join.
-- -------------------------------------------------------------------------------------------------

SET 'pipeline.name' = 'q20_unique';

CREATE TABLE nexmark_q20 (
    bid_id  BIGINT,
    auction_event_id  BIGINT,
    auction_id  BIGINT,
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    channel  VARCHAR,
    url  VARCHAR,
    bid_dateTime  TIMESTAMP(3),
    bid_extra  VARCHAR,

    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    auction_dateTime  TIMESTAMP(3),
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    auction_extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
--   'print-identifier' = 'nexmark_q20'
);

INSERT INTO nexmark_q20
SELECT
    B.bid_id,
    A.auction_event_id,
    A.id AS auction_id,
    auction,
    bidder,
    price,
    channel,
    url,
    B.`dateTime`,
    B.extra,
    itemName,
    description,
    initialBid,
    reserve,
    A.`dateTime`,
    expires,
    seller,
    category,
    A.extra
FROM
    bid AS B INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10;
