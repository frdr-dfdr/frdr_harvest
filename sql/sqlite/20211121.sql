alter table descriptions add column record_uuid TEXT;
alter table domain_metadata add column record_uuid TEXT;
alter table geobbox add column record_uuid TEXT;
alter table geofile add column record_uuid TEXT;
alter table geopoint add column record_uuid TEXT;
alter table geospatial add column record_uuid TEXT;
alter table records add column record_uuid TEXT;
alter table records_x_access add column record_uuid TEXT;
alter table records_x_affiliations add column record_uuid TEXT;
alter table records_x_crdc add column record_uuid TEXT;
alter table records_x_creators add column record_uuid TEXT;
alter table records_x_geoplace add column record_uuid TEXT;
alter table records_x_publishers add column record_uuid TEXT;
alter table records_x_rights add column record_uuid TEXT;
alter table records_x_subjects add column record_uuid TEXT;
alter table records_x_tags add column record_uuid TEXT;