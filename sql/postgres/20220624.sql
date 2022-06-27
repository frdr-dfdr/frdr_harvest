UPDATE repositories
SET repository_url = REPLACE(repository_url,'dataverse.scholarsportal.info','borealisdata.ca')
WHERE repository_url like '%scholarsportal.info%';

UPDATE repositories
SET homepage_url = REPLACE(homepage_url,'dataverse.scholarsportal.info','borealisdata.ca')
WHERE homepage_url like '%scholarsportal.info%';

update records set modified_timestamp = 0 where repository_id in (
    select repository_id from repositories where repository_url like '%borealisdata.ca%'
);

update repositories set repository_name = 'Other Borealis Dataverses'
where repository_name = 'Other Scholars Portal Dataverses';

UPDATE repositories
SET repository_thumbnail = REPLACE(repository_thumbnail,'sp_80x80','borealis_80x80')
WHERE repository_name = 'Other Borealis Dataverses';
