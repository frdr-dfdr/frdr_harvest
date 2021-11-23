-- records

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_temp (
    record_uuid TEXT PRIMARY KEY NOT NULL,
    repository_id INTEGER NOT NULL,
    title TEXT,
    pub_date TEXT,
    modified_timestamp INTEGER DEFAULT 0,
    source_url TEXT,
    deleted INTEGER DEFAULT 0,
    local_identifier TEXT,
    series TEXT,
    item_url TEXT,
    title_fr TEXT,
    upstream_modified_timestamp INTEGER DEFAULT 0,
    geodisy_harvested INTEGER DEFAULT 0, 
    files_altered INTEGER DEFAULT 1, 
    files_size INTEGER DEFAULT 0
);

INSERT INTO records_temp SELECT 
    record_uuid,
    repository_id,
    title,
    pub_date,
    modified_timestamp,
    source_url,
    deleted,
    local_identifier,
    series,
    item_url,
    title_fr,
    upstream_modified_timestamp,
    geodisy_harvested,
    files_altered,
    files_size
    FROM records;

DROP INDEX IF EXISTS records_by_modified_timestamp;

DROP TABLE records;
ALTER TABLE records_temp RENAME TO records;

CREATE INDEX IF NOT EXISTS records_by_modified_timestamp ON records(modified_timestamp);

COMMIT;

--  descriptions

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS descriptions_temp (
    description_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    description TEXT,
    description_hash TEXT,
    language TEXT
);

INSERT INTO descriptions_temp SELECT 
    description_id,
    record_uuid,
    description,
    description_hash,
    language
    FROM descriptions;

DROP INDEX IF EXISTS descriptions_by_description_hash;
DROP INDEX IF EXISTS descriptions_by_record_id;

DROP TABLE descriptions;
ALTER TABLE descriptions_temp RENAME TO descriptions;

CREATE INDEX descriptions_by_description_hash on descriptions(description_hash);
CREATE INDEX descriptions_by_record on descriptions(record_uuid, language);

COMMIT;

--  domain_metadata

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS domain_metadata_temp (
    metadata_id INTEGER PRIMARY KEY NOT NULL,
    schema_id INTEGER NOT NULL,
    record_uuid TEXT NOT NULL,
    field_name TEXT,
    field_value TEXT
);

INSERT INTO domain_metadata_temp SELECT 
    metadata_id,
    schema_id,
    record_uuid,
    field_name,
    field_value
    FROM domain_metadata;

DROP INDEX IF EXISTS domain_metadata_by_record;

DROP TABLE domain_metadata;
ALTER TABLE domain_metadata_temp RENAME TO domain_metadata;

CREATE INDEX domain_metadata_by_record on domain_metadata(record_uuid,schema_id);

COMMIT;

--  geobbox

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS geobbox_temp (
    geobbox_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    westLon NUMERIC NOT NULL,
    eastLon NUMERIC NOT NULL,
    northLat NUMERIC NOT NULL,
    southLat NUMERIC NOT NULL
);

INSERT INTO geobbox_temp SELECT
    geobbox_id,
    record_uuid,
    westLon,
    eastLon,
    northLat,
    southLat
    FROM geobbox;

DROP INDEX IF EXISTS geobbox_by_record;

DROP TABLE geobbox;
ALTER TABLE geobbox_temp RENAME TO geobbox;

CREATE INDEX geobbox_by_record on geobbox(record_uuid);

COMMIT;

--  geofile

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS geofile_temp (
    geofile_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    filename TEXT NOT NULL,
    uri TEXT NOT NULL
);

INSERT INTO geofile_temp SELECT
    geofile_id,
    record_uuid,
    filename,
    uri
    FROM geofile;

DROP INDEX IF EXISTS geofileby_record;

DROP TABLE geofile;
ALTER TABLE geofile_temp RENAME TO geofile;

CREATE INDEX geofileby_record on geofile(record_uuid);

COMMIT;

--  geopoint

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS geopoint_temp (
    geopoint_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    lat NUMERIC NOT NULL,
    lon NUMERIC NOT NULL
);

INSERT INTO geopoint_temp SELECT
    geopoint_id,
    record_uuid,
    lat,
    lon
    FROM geopoint;

DROP INDEX IF EXISTS geopoint_by_record;

DROP TABLE geopoint;
ALTER TABLE geopoint_temp RENAME TO geopoint;

CREATE INDEX geopoint_by_record on geopoint(record_uuid);

COMMIT;

--  geospatial

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS geospatial_temp (
    geospatial_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    coordinate_type TEXT,
    lat NUMERIC,
    lon NUMERIC
);

INSERT INTO geospatial_temp SELECT
    geospatial_id,
    record_uuid,
    coordinate_type,
    lat,
    lon
    FROM geospatial;

DROP INDEX IF EXISTS geospatial_by_record;

DROP TABLE geospatial;
ALTER TABLE geospatial_temp RENAME TO geospatial;

CREATE INDEX geospatial_by_record on geospatial(record_uuid);

COMMIT;

--  records_x_access

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_access_temp (
    records_x_access_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    access_id INTEGER NOT NULL
);

INSERT INTO records_x_access_temp SELECT
    records_x_access_id,
    record_uuid,
    access_id
    FROM records_x_access;

DROP INDEX IF EXISTS records_x_access_by_record;
DROP INDEX IF EXISTS records_x_access_by_access;

DROP TABLE records_x_access;
ALTER TABLE records_x_access_temp RENAME TO records_x_access;

CREATE INDEX records_x_access_by_record on records_x_access(record_uuid);
CREATE INDEX records_x_access_by_access on records_x_access(access_id);

COMMIT;

--  records_x_affiliations

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_affiliations_temp (
    records_x_affiliations_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    affiliation_id INTEGER NOT NULL
);

INSERT INTO records_x_affiliations_temp SELECT
    records_x_affiliations_id,
    record_uuid,
    affiliation_id
    FROM records_x_affiliations;

DROP INDEX IF EXISTS records_x_affiliations_by_record;
DROP INDEX IF EXISTS records_x_affiliations_by_affiliation;

DROP TABLE records_x_affiliations;
ALTER TABLE records_x_affiliations_temp RENAME TO records_x_affiliations;

CREATE INDEX records_x_affiliations_by_record on records_x_affiliations(record_uuid);
CREATE INDEX records_x_affiliations_by_affiliation on records_x_affiliations(affiliation_id);

COMMIT;

--  records_x_crdc

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_crdc_temp (
    records_x_crdc_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    crdc_id INTEGER NOT NULL
);

INSERT INTO records_x_crdc_temp SELECT
    records_x_crdc_id,
    record_uuid,
    crdc_id
    FROM records_x_crdc;

DROP INDEX IF EXISTS records_x_crdc_by_record;
DROP INDEX IF EXISTS records_x_crdc_by_crdc;

DROP TABLE records_x_crdc;
ALTER TABLE records_x_crdc_temp RENAME TO records_x_crdc;

CREATE INDEX records_x_crdc_by_record on records_x_crdc(record_uuid);
CREATE INDEX records_x_crdc_by_crdc on records_x_crdc(crdc_id);

COMMIT;

--  records_x_creators

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_creators_temp (
    records_x_creators_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    creator_id INTEGER NOT NULL,
    is_contributor INTEGER NOT NULL
);

INSERT INTO records_x_creators_temp SELECT
    records_x_creators_id,
    record_uuid,
    creator_id,
    is_contributor
    FROM records_x_creators;

DROP INDEX IF EXISTS records_x_creators_by_record;
DROP INDEX IF EXISTS records_x_creators_by_creator;

DROP TABLE records_x_creators;
ALTER TABLE records_x_creators_temp RENAME TO records_x_creators;

CREATE INDEX records_x_creators_by_record on records_x_creators(record_uuid);
CREATE INDEX records_x_creators_by_creator on records_x_creators(creator_id);

COMMIT;

--  records_x_geoplace

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_geoplace_temp (
    records_x_geoplace_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    geoplace_id INTEGER NOT NULL
);

INSERT INTO records_x_geoplace_temp SELECT
    records_x_geoplace_id,
    record_uuid,
    geoplace_id
    FROM records_x_geoplace;

DROP INDEX IF EXISTS records_x_geoplace_by_record;
DROP INDEX IF EXISTS records_x_geoplace_by_geoplace;

DROP TABLE records_x_geoplace;
ALTER TABLE records_x_geoplace_temp RENAME TO records_x_geoplace;

CREATE INDEX records_x_geoplace_by_record on records_x_geoplace(record_uuid);
CREATE INDEX records_x_geoplace_by_geoplace on records_x_geoplace(geoplace_id);

COMMIT;

--  records_x_publishers

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_publishers_temp (
    records_x_publishers_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    publisher_id INTEGER NOT NULL
);

INSERT INTO records_x_publishers_temp SELECT
    records_x_publishers_id,
    record_uuid,
    publisher_id
    FROM records_x_publishers;

DROP INDEX IF EXISTS records_x_publishers_by_record;
DROP INDEX IF EXISTS records_x_publishers_by_publisher;

DROP TABLE records_x_publishers;
ALTER TABLE records_x_publishers_temp RENAME TO records_x_publishers;

CREATE INDEX records_x_publishers_by_record on records_x_publishers(record_uuid);
CREATE INDEX records_x_publishers_by_publisher on records_x_publishers(publisher_id);

COMMIT;

--  records_x_rights

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_rights_temp (
    records_x_rights_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    rights_id INTEGER NOT NULL
);

INSERT INTO records_x_rights_temp SELECT
    records_x_rights_id,
    record_uuid,
    rights_id
    FROM records_x_rights;

DROP INDEX IF EXISTS records_x_rights_by_record;
DROP INDEX IF EXISTS records_x_rights_by_right;

DROP TABLE records_x_rights;
ALTER TABLE records_x_rights_temp RENAME TO records_x_rights;

CREATE INDEX records_x_rights_by_record on records_x_rights(record_uuid);
CREATE INDEX records_x_rights_by_right on records_x_rights(rights_id);

COMMIT;

--  records_x_subjects

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_subjects_temp (
    records_x_subjects_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    subject_id INTEGER NOT NULL
);

INSERT INTO records_x_subjects_temp SELECT
    records_x_subjects_id,
    record_uuid,
    subject_id
    FROM records_x_subjects;

DROP INDEX IF EXISTS records_x_subjects_by_record;
DROP INDEX IF EXISTS records_x_subjects_by_subject;

DROP TABLE records_x_subjects;
ALTER TABLE records_x_subjects_temp RENAME TO records_x_subjects;

CREATE INDEX records_x_subjects_by_record on records_x_subjects(record_uuid);
CREATE INDEX records_x_subjects_by_subject on records_x_subjects(subject_id);

COMMIT;

--  records_x_tags

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS records_x_tags_temp (
    records_x_tags_id INTEGER PRIMARY KEY NOT NULL,
    record_uuid TEXT NOT NULL,
    tag_id INTEGER NOT NULL
);

INSERT INTO records_x_tags_temp SELECT
    records_x_tags_id,
    record_uuid,
    tag_id
    FROM records_x_tags;

DROP INDEX IF EXISTS records_x_tags_by_record;
DROP INDEX IF EXISTS records_x_tags_by_tag;

DROP TABLE records_x_tags;
ALTER TABLE records_x_tags_temp RENAME TO records_x_tags;

CREATE INDEX records_x_tags_by_record on records_x_tags(record_uuid);
CREATE INDEX records_x_tags_by_tag on records_x_tags(tag_id);

COMMIT;

VACUUM;
