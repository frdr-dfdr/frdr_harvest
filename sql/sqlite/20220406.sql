create unique index if not exists repositories_by_name on repositories(repository_name);

delete from subjects where subject is null;
drop index if exists subjects_by_subject;
create unique index if not exists subjects_by_subject_language on subjects(subject,language);

delete from tags where tag is null;
delete from tags where tag = '';
create unique index if not exists tags_by_tag_language on tags(tag,language);

delete from publishers where publisher is null;
drop index if exists publishers_by_publisher;
create unique index if not exists publishers_by_publisher on publishers(publisher);

delete from access where access is null;
drop index if exists access_by_access;
create unique index if not exists access_by_access on access(access);

delete from rights where rights is null;
drop index if exists rights_by_right_hash;
create unique index if not exists rights_by_rights on rights(rights);
create unique index if not exists rights_by_right_hash on rights(rights_hash);

delete from affiliations where affiliation is null;
drop index if exists affiliations_by_affiliation;
create unique index if not exists affiliations_by_affiliation_ror on affiliations(affiliation,affiliation_ror);

DELETE FROM records_x_access
WHERE records_x_access_id NOT IN (
    SELECT MAX(records_x_access_id) AS MaxID
    FROM records_x_access
    GROUP BY record_uuid,access_id
);
drop index if exists records_x_access_by_access;
drop index if exists records_x_access_by_record;
create unique index if not exists records_x_access_by_record_access on records_x_access(record_uuid,access_id);

DELETE FROM records_x_crdc
WHERE records_x_crdc_id NOT IN (
    SELECT MAX(records_x_crdc_id) AS MaxID
    FROM records_x_crdc
    GROUP BY record_uuid,crdc_id
);
drop index if exists records_x_crdc_by_record;
create unique index if not exists records_x_crdc_by_record_crdc on records_x_crdc(record_uuid,crdc_id);

DELETE FROM records_x_affiliations
WHERE records_x_affiliations_id NOT IN (
    SELECT MAX(records_x_affiliations_id) AS MaxID
    FROM records_x_affiliations
    GROUP BY record_uuid,affiliation_id
);
drop index if exists records_x_affiliations_by_affiliation;
drop index if exists records_x_affiliations_by_record;
create unique index if not exists records_x_affiliations_by_record_affiliation on records_x_affiliations(record_uuid,affiliation_id);

DELETE FROM records_x_creators
WHERE records_x_creators_id NOT IN (
    SELECT MAX(records_x_creators_id) AS MaxID
    FROM records_x_creators
    GROUP BY record_uuid,creator_id,is_contributor
);
drop index if exists records_x_creators_by_creator;
drop index if exists records_x_creators_by_record;
create unique index if not exists records_x_creators_by_record_creator on records_x_creators(record_uuid,creator_id);

DELETE FROM records_x_publishers
WHERE records_x_publishers_id NOT IN (
    SELECT MAX(records_x_publishers_id) AS MaxID
    FROM records_x_publishers
    GROUP BY record_uuid,publisher_id
);
drop index if exists records_x_publishers_by_publisher;
drop index if exists records_x_publishers_by_record;
create unique index if not exists records_x_publishers_by_record_publisher on records_x_publishers(record_uuid,publisher_id);

DELETE FROM records_x_rights
WHERE records_x_rights_id NOT IN (
    SELECT MAX(records_x_rights_id) AS MaxID
    FROM records_x_rights
    GROUP BY record_uuid,rights_id
);
drop index if exists records_x_rights_by_record;
drop index if exists records_x_rights_by_right;
create unique index if not exists records_x_rights_by_record_right on records_x_rights(record_uuid,rights_id);

DELETE FROM records_x_subjects
WHERE records_x_subjects_id NOT IN (
    SELECT MAX(records_x_subjects_id) AS MaxID
    FROM records_x_subjects
    GROUP BY record_uuid,subject_id
);
drop index if exists records_x_subjects_by_record;
drop index if exists records_x_subjects_by_subject;
create unique index if not exists records_x_subjects_by_record_subject on records_x_subjects(record_uuid,subject_id);

DELETE FROM records_x_tags
WHERE records_x_tags_id NOT IN (
    SELECT MAX(records_x_tags_id) AS MaxID
    FROM records_x_tags
    GROUP BY record_uuid,tag_id
);
drop index if exists records_x_tags_by_record;
drop index if exists records_x_tags_by_tag;
create unique index if not exists records_x_tags_by_record_tag on records_x_tags(record_uuid,tag_id);
