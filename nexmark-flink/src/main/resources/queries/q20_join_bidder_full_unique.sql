CREATE TABLE nexmark_q20_join_bidder_full_unique (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    `dateTime` TIMESTAMP(3),
    auction_id BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20_join_bidder_full_unique
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime`,
    A.id
FROM bid AS B
INNER JOIN auction AS A
ON B.bidder = A.id
WHERE A.category = 10;
