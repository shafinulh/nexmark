-- -------------------------------------------------------------------------------------------------
-- Query 4: Average Price for a Category (Unique)
-- -------------------------------------------------------------------------------------------------
-- Select the average of the winning bid prices for all auctions in each category.
-- Mirrors q4 semantics while keeping unique IDs available on both join inputs.
-- -------------------------------------------------------------------------------------------------

SET 'pipeline.name' = 'q4_unique';

CREATE TABLE nexmark_q4 (
  id BIGINT,
  final BIGINT
) WITH (
  'connector' = 'blackhole'
  -- 'print-identifier' = 'nexmark_q4'
);

INSERT INTO nexmark_q4
SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT
        J.category,
        MAX(J.price) AS final,
        MIN(J.auction_event_id) AS auction_event_id_anchor,
        MIN(J.bid_id) AS bid_id_anchor
    FROM (
        SELECT
            A.auction_event_id,
            A.id,
            A.`dateTime`,
            A.expires,
            A.category,
            B.bid_id,
            B.auction,
            B.price,
            B.`dateTime` AS bid_dateTime
        FROM (
            SELECT
                auction_event_id,
                id,
                category,
                `dateTime`,
                expires
            FROM auction
        ) A
        JOIN (
            SELECT
                bid_id,
                auction,
                price,
                `dateTime`
            FROM bid
        ) B
        ON A.id = B.auction
        WHERE B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    ) J
    GROUP BY J.id, J.category
) Q
GROUP BY Q.category;
