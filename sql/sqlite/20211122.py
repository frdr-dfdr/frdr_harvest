import uuid

# Migration 20211122: 
#   - determine UUID for all records and update them
#   - populate all related tables with UUIDs
#
# UUID5 was chosen because for the same namespace and string (item_url)
# it always calculates the same value
#

class Migration:
    db = None
    tables_to_update = ["descriptions","domain_metadata","geobbox","geofile","geopoint","geospatial","records","records_x_access","records_x_affiliations",
        "records_x_crdc","records_x_creators","records_x_geoplace","records_x_publishers","records_x_rights","records_x_subjects","records_x_tags"]

    def set_dbinterface(self, p):
        self.db = p

    def migrate(self):
        # This SQL will only work once, before the next migration is done. Afterwards, record_id column is gone.
        select_sql = """SELECT recs.record_id, recs.source_url, recs.deleted, recs.local_identifier, recs.item_url,
            repos.repository_url, repos.item_url_pattern
            FROM records recs, repositories repos WHERE recs.repository_id = repos.repository_id"""
        records_to_update = self.db.get_records_raw_query(select_sql)

        for row in records_to_update:
            # Convert row object to a modifiable dict
            record = {
                "record_id": row["record_id"],
                "source_url": row["source_url"],
                "deleted": row["deleted"],
                "local_identifier": row["local_identifier"],
                "item_url": row["item_url"],
                "repository_url": row["repository_url"],
                "item_url_pattern": row["item_url_pattern"]
            }

            if record["item_url"] == "":
                record["item_url"] = self.db.construct_local_url(record)

            if record["item_url"] is None:
                print("Deleting record {} {} from {}".format(record["record_id"], record["local_identifier"], record["repository_url"]))
                self.db.delete_record(record)
                continue

            new_uuid = self.db.get_uuid(record["item_url"])

            # Sanity check to make sure we did not generate the UUID for a blank URL
            if new_uuid != "1b4db7eb-4057-5ddf-91e0-36dec72071f5":
                self.db.update_records_raw_query("update records set record_uuid='{}' where record_id={};".format(new_uuid,str(record["record_id"])))
                for tablename in self.tables_to_update:
                    self.db.update_records_raw_query("""update {} set record_uuid='{}' where record_id={};""".format(tablename,new_uuid,str(record["record_id"])))

        self.db.purge_deleted_records()