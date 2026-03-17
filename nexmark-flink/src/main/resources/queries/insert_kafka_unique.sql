-- -------------------------------------------------------------------------------------------------
-- Insert into three Kafka topics from the unique nexmark data generator.
-- -------------------------------------------------------------------------------------------------

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO person_kafka
    SELECT id, name, emailAddress, creditCard, city, state, `dateTime`, extra
    FROM person_src;

    INSERT INTO auction_kafka
    SELECT id, itemName, description, initialBid, reserve, `dateTime`, expires, seller, category, extra
    FROM auction_src;

    INSERT INTO bid_kafka
    SELECT id, auction, bidder, price, channel, url, `dateTime`, extra
    FROM bid_src;
END;
