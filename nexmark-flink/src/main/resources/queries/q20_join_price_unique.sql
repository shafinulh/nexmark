CREATE TABLE nexmark_q20_join_price_unique (
    price BIGINT,
    auction_id BIGINT
) WITH (
    'connector' = 'blackhole'
);

INSERT INTO nexmark_q20_join_price_unique
SELECT
    B.price,
    A.id
FROM bid AS B
INNER JOIN auction AS A
ON B.price = A.id
WHERE A.category = 10;
