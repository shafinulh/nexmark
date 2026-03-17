CREATE VIEW person AS
SELECT
    id,
    name,
    emailAddress,
    creditCard,
    city,
    state,
    `dateTime`,
    extra
FROM ${PERSON_TABLE};

CREATE VIEW auction AS
SELECT
    id,
    itemName,
    description,
    initialBid,
    reserve,
    `dateTime`,
    expires,
    seller,
    category,
    extra
FROM ${AUCTION_TABLE};

CREATE VIEW bid AS
SELECT
    auction,
    bidder,
    price,
    channel,
    url,
    `dateTime`,
    extra
FROM ${BID_TABLE};

CREATE VIEW bid_modified AS
SELECT
    id,
    auction,
    bidder,
    price,
    channel,
    url,
    `dateTime`,
    extra
FROM ${BID_MODIFIED_TABLE};
