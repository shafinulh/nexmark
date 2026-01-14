-- -------------------------------------------------------------------------------------------------
-- Query 9: Winning Bids (Not in original suite)
-- -------------------------------------------------------------------------------------------------
-- Find the winning bid for each auction.
-- Emits the unique bid identifier so both sides of the join provide a unique key.
-- -------------------------------------------------------------------------------------------------

SET 'pipeline.name' = 'q9_unique';

CREATE TABLE nexmark_q9 (
  bid_id  BIGINT,
  id  BIGINT,
  itemName  VARCHAR,
  description  VARCHAR,
  initialBid  BIGINT,
  reserve  BIGINT,
  `dateTime`  TIMESTAMP(3),
  expires  TIMESTAMP(3),
  seller  BIGINT,
  category  BIGINT,
  extra  VARCHAR,
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  bid_dateTime  TIMESTAMP(3),
  bid_extra  VARCHAR
) WITH (
  'connector' = 'blackhole'
  -- 'print-identifier' = 'nexmark_q9'
);

INSERT INTO nexmark_q9
SELECT
    bid_id, id, itemName, description, initialBid, reserve, `dateTime`, expires,
    seller, category, extra,
    auction, bidder, price, bid_dateTime, bid_extra
FROM (
   SELECT
     B.bid_id,
     A.*,
     B.auction,
     B.bidder,
     B.price,
     B.`dateTime` AS bid_dateTime,
     B.extra AS bid_extra,
     ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.`dateTime` ASC) AS rownum
   FROM auction A, bid B
   WHERE A.id = B.auction AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
)
WHERE rownum <= 1;
