-- records

BEGIN TRANSACTION;

ALTER TABLE records DROP CONSTRAINT IF EXISTS records_pkey;
ALTER TABLE records DROP COLUMN IF EXISTS record_id;
CREATE UNIQUE INDEX IF NOT EXISTS records_pkey ON records (record_uuid);

COMMIT;

--  descriptions

BEGIN TRANSACTION;

DROP INDEX IF EXISTS descriptions_by_record_id;
ALTER TABLE descriptions DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS descriptions_by_record_id on descriptions(record_uuid, language);

COMMIT;

--  domain_metadata

BEGIN TRANSACTION;

DROP INDEX IF EXISTS domain_metadata_by_record;
ALTER TABLE domain_metadata DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS domain_metadata_by_record on domain_metadata(record_uuid,schema_id);

COMMIT;

--  geobbox

BEGIN TRANSACTION;

DROP INDEX IF EXISTS geobbox_by_record;
ALTER TABLE geobbox DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS geobbox_by_record on geobbox(record_uuid);

COMMIT;

--  geofile

BEGIN TRANSACTION;


DROP INDEX IF EXISTS geofileby_record;
ALTER TABLE geofile DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS geofileby_record on geofile(record_uuid);

COMMIT;

--  geopoint

BEGIN TRANSACTION;

DROP INDEX IF EXISTS geopoint_by_record;
ALTER TABLE geopoint DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS geopoint_by_record on geopoint(record_uuid);

COMMIT;

--  records_x_access

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_access_by_record;
ALTER TABLE records_x_access DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_access_by_record on records_x_access(record_uuid);

COMMIT;

--  records_x_affiliations

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_affiliations_by_record;
ALTER TABLE records_x_affiliations DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_affiliations_by_record on records_x_affiliations(record_uuid);

COMMIT;

--  records_x_crdc

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_crdc_by_record;
ALTER TABLE records_x_crdc DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_crdc_by_record on records_x_crdc(record_uuid);

COMMIT;

--  records_x_creators

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_creators_by_record;
ALTER TABLE records_x_creators DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_creators_by_record on records_x_creators(record_uuid);

COMMIT;

--  records_x_geoplace

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_geoplace_by_record;
ALTER TABLE records_x_geoplace DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_geoplace_by_record on records_x_geoplace(record_uuid);

COMMIT;

--  records_x_publishers

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_publishers_by_record;
ALTER TABLE records_x_publishers DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_publishers_by_record on records_x_publishers(record_uuid);

COMMIT;

--  records_x_rights

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_rights_by_record;
ALTER TABLE records_x_rights DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_rights_by_record on records_x_rights(record_uuid);

COMMIT;

--  records_x_subjects

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_subjects_by_record;
ALTER TABLE records_x_subjects DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_subjects_by_record on records_x_subjects(record_uuid);

COMMIT;

--  records_x_tags

BEGIN TRANSACTION;

DROP INDEX IF EXISTS records_x_tags_by_record;
ALTER TABLE records_x_tags DROP COLUMN IF EXISTS record_id;
CREATE INDEX IF NOT EXISTS records_x_tags_by_record on records_x_tags(record_uuid);

COMMIT;

