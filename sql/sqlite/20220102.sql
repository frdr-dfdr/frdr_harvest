CREATE TABLE geospatial_boundingbox(
	geospatial_boundingbox_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	geofile_id INTEGER,
	westLon NUMERIC NOT NULL,
	eastLon NUMERIC NOT NULL,
    northLat NUMERIC NOT NULL,
    southLat NUMERIC NOT NULL);

INSERT INTO temp SELECT * FROM geospatial_boundingbox;
DROP INDEX geospatial_boundingbox_by_record;
DROP TABLE geospatial_boundingbox;
ALTER TABLE temp TO geospatial_boundingbox;
CREATE INDEX geospatial_boundingbox_by_record on geospatial_boundingbox(record_id);

vacuum;