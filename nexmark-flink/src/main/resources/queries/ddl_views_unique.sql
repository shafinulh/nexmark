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
FROM person_src;

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
FROM auction_src;

CREATE VIEW bid AS
SELECT
    auction,
    bidder,
    price,
    channel,
    url,
    `dateTime`,
    extra
FROM bid_src;

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
FROM bid_modified_src;
