CREATE TABLE nexmark_q20_join_bidder_unique (
    bidder BIGINT,
    auction_id BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20_join_bidder_unique
SELECT
    B.bidder,
    A.id
FROM bid AS B
INNER JOIN auction AS A
ON B.bidder = A.id
WHERE A.category = 10;
