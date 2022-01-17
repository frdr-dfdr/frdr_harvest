CREATE TABLE IF NOT EXISTS geonames (
geonames_id INTEGER PRIMARY KEY NOT NULL,
country text,
province_state text,
city text,
northLat NUMERIC,
southLat NUMERIC,
eastLon NUMERIC,
westLon NUMERIC,
last_modified_timestamp int DEFAULT 0);

ALTER TABLE geoplace
ADD COLUMN record_uuid TEXT;

ALTER TABLE geoplace
ADD COLUMN geonames_id INTEGER;

ALTER TABLE geoplace
ADD COLUMN upstream_modified_timestamp INTEGER DEFAULT 0;

ALTER TABLE geoplace
ADD COLUMN geonames_associated_timestamp INTEGER DEFAULT 0;

ALTER TABLE geoplace
ADD COLUMN geodisy_review_status INTEGER DEFAULT 0;

UPDATE geoplace set record_uuid = (select record_uuid from records_x_geoplace where records_x_geoplace.geoplace_id = geoplace.geoplace_id limit 1);

DROP INDEX IF EXISTS records_x_geoplace_by_record;
DROP INDEX IF EXISTS records_x_geoplace_by_geoplace;

DROP TABLE records_x_geoplace;

ALTER TABLE geobbox
ADD COLUMN geofile_id INTEGER;