-- -------------------------------------------------------------------------------------------------
-- Insert into kafka from nexmark data generator.
-- -------------------------------------------------------------------------------------------------

INSERT INTO kafka
SELECT event_type, event_id, person, auction, bid FROM datagen;
