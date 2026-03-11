CREATE TABLE person_src (
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
    'connector' = 'nexmark',
    'event.type' = 'person',
    'base-time' = '${BASE_TIME_MILLIS}',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'events.num' = '${EVENTS_NUM}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
);

CREATE TABLE auction_src (
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
    'connector' = 'nexmark',
    'event.type' = 'auction',
    'base-time' = '${BASE_TIME_MILLIS}',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'events.num' = '${EVENTS_NUM}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
);

CREATE TABLE bid_src (
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
    'connector' = 'nexmark',
    'event.type' = 'bid',
    'base-time' = '${BASE_TIME_MILLIS}',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'events.num' = '${EVENTS_NUM}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
);

CREATE TABLE bid_modified_src (
    id BIGINT,
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'nexmark',
    'event.type' = 'bid',
    'base-time' = '${BASE_TIME_MILLIS}',
    'first-event.rate' = '${TPS}',
    'next-event.rate' = '${TPS}',
    'events.num' = '${EVENTS_NUM}',
    'person.proportion' = '${PERSON_PROPORTION}',
    'auction.proportion' = '${AUCTION_PROPORTION}',
    'bid.proportion' = '${BID_PROPORTION}'
);
