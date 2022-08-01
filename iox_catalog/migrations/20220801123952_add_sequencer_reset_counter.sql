-- Add migration script here
ALTER TABLE sequencer ADD COLUMN reset_count INT;

UPDATE sequencer SET reset_count = 0;

ALTER TABLE sequencer ALTER COLUMN reset_count SET NOT NULL;
