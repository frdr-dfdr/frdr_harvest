CONSTRAINT fk_geonames REFERENCES geonames(geonames_id)
ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE geoplace
ADD COLUMN upstream_modified_timestamp INTEGER DEFAULT 0;

ALTER TABLE geoplace
ADD COLUMN geonames_associated_timestamp INTEGER DEFAULT 0;

ALTER TABLE geoplace
ADD COLUMN geodisy_review_status INTEGER DEFAULT 0;

UPDATE geoplace set record_uuid = (select record_uuid from records_x_geoplace where records_x_geoplace.geoplace_id = geoplace.geoplace_id);

DROP TABLE records_x_geoplace;