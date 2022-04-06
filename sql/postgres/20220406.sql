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

DELETE FROM records_x_access a USING records_x_access b
WHERE
    a.records_x_access_id < b.records_x_access_id AND
    a.access_id = b.access_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_access_by_access;
drop index if exists records_x_access_by_record;
create unique index if not exists records_x_access_by_record_access on records_x_access(record_uuid,access_id);

DELETE FROM records_x_crdc a USING records_x_crdc b
WHERE
    a.records_x_crdc_id < b.records_x_crdc_id AND
    a.crdc_id = b.crdc_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_crdc_by_record;
create unique index if not exists records_x_crdc_by_record_crdc on records_x_crdc(record_uuid,crdc_id);

DELETE FROM records_x_affiliations a USING records_x_affiliations b
WHERE
    a.records_x_affiliations_id < b.records_x_affiliations_id AND
    a.affiliation_id = b.affiliation_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_affiliations_by_affiliation;
drop index if exists records_x_affiliations_by_record;
create unique index if not exists records_x_affiliations_by_record_affiliation on records_x_affiliations(record_uuid,affiliation_id);

DELETE FROM records_x_creators a USING records_x_creators b
WHERE
    a.records_x_creators_id < b.records_x_creators_id AND
    a.creator_id = b.creator_id AND
    a.is_contributor = b.is_contributor AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_creators_by_creator;
drop index if exists records_x_creators_by_record;
create unique index if not exists records_x_creators_by_record_creator on records_x_creators(record_uuid,creator_id,is_contributor);

DELETE FROM records_x_publishers a USING records_x_publishers b
WHERE
    a.records_x_publishers_id < b.records_x_publishers_id AND
    a.publisher_id = b.publisher_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_publishers_by_publisher;
drop index if exists records_x_publishers_by_record;
create unique index if not exists records_x_publishers_by_record_publisher on records_x_publishers(record_uuid,publisher_id);

DELETE FROM records_x_rights a USING records_x_rights b
WHERE
    a.records_x_rights_id < b.records_x_rights_id AND
    a.rights_id = b.rights_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_rights_by_record;
drop index if exists records_x_rights_by_right;
create unique index if not exists records_x_rights_by_record_right on records_x_rights(record_uuid,rights_id);

DELETE FROM records_x_subjects a USING records_x_subjects b
WHERE
    a.records_x_subjects_id < b.records_x_subjects_id AND
    a.subject_id = b.subject_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_subjects_by_record;
drop index if exists records_x_subjects_by_subject;
create unique index if not exists records_x_subjects_by_record_subject on records_x_subjects(record_uuid,subject_id);

DELETE FROM records_x_tags a USING records_x_tags b
WHERE
    a.records_x_tags_id < b.records_x_tags_id AND
    a.tag_id = b.tag_id AND
    a.record_uuid = b.record_uuid;
drop index if exists records_x_tags_by_record;
drop index if exists records_x_tags_by_tag;
create unique index if not exists records_x_tags_by_record_tag on records_x_tags(record_uuid,tag_id);
