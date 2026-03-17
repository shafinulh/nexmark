CREATE TABLE person_kafka (
    id BIGINT,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'nexmark-person',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'nexmark',
    'scan.watermark.emit.strategy' = 'on-event',
    'scan.watermark.idle-timeout' = '30s',
    'scan.watermark.alignment.group' = 'nexmark-events',
    'scan.watermark.alignment.max-drift' = '1s',
    'scan.watermark.alignment.update-interval' = '200ms',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TABLE auction_kafka (
    id BIGINT,
    itemName VARCHAR,
    description VARCHAR,
    initialBid BIGINT,
    reserve BIGINT,
    `dateTime` TIMESTAMP(3),
    expires TIMESTAMP(3),
    seller BIGINT,
    category BIGINT,
    extra VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'nexmark-auction',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'nexmark',
    'scan.watermark.emit.strategy' = 'on-event',
    'scan.watermark.idle-timeout' = '30s',
    'scan.watermark.alignment.group' = 'nexmark-events',
    'scan.watermark.alignment.max-drift' = '1s',
    'scan.watermark.alignment.update-interval' = '200ms',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TABLE bid_kafka (
    id BIGINT,
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND,
    PRIMARY KEY (auction, bidder, price, `dateTime`) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'nexmark-bid',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'nexmark',
    'scan.watermark.emit.strategy' = 'on-event',
    'scan.watermark.idle-timeout' = '30s',
    'scan.watermark.alignment.group' = 'nexmark-events',
    'scan.watermark.alignment.max-drift' = '1s',
    'scan.watermark.alignment.update-interval' = '200ms',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY'
);
