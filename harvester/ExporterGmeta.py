import re
import json
import harvester.Exporter as Exporter
from psycopg2.extras import DictCursor


class ExporterGmeta(Exporter.Exporter):
    """ Read records from the database and export to gmeta """

    def __init__(self, db, log, finalconfig):
        self.export_format = "gmeta"
        super().__init__(db, log, finalconfig)

    def _generate(self, only_new_records):
        self.logger.info("Exporter: generate called for gmeta")
        self.output_buffer = []
        deleted = []
        recordidcolumn = self.db.get_table_id_column("records")

        try:
            lastrun_timestamp = int(self.db.get_setting("last_run_timestamp"))
        except Exception as e:
            lastrun_timestamp = 0

        records_con = self.db.getConnection()
        with records_con:
            records_cursor = self.db.getRowCursor()

        records_sql = """SELECT recs.""" + recordidcolumn + """, recs.title, recs.title_fr, recs.pub_date, recs.series,
            recs.deleted, recs.local_identifier, recs.item_url, recs.modified_timestamp,
            repos.repository_url, repos.repository_name, repos.repository_name_fr, repos.repository_thumbnail, repos.item_url_pattern, repos.last_crawl_timestamp
            FROM records recs, repositories repos WHERE recs.repository_id = repos.repository_id"""
        records_args = ()

        if self.export_repository_id:
            records_sql += " AND repos.repository_id = ?"
            records_args = records_args + (int(self.export_repository_id),)

        if only_new_records:
            records_sql += " AND recs.modified_timestamp >= ?"
            records_args = records_args + (lastrun_timestamp,)

        if len(records_args):
            records_cursor.execute(self.db._prep(records_sql), records_args)
        else:
            records_cursor.execute(self.db._prep(records_sql))

        buffer_limit = int(self.export_limit) * 1024 * 1024
        self.logger.info("Exporter: output file size limited to {} MB each".format(int(self.export_limit)))

        records_assembled = 0
        self.batch_number = 1
        self.buffer_size = 0
        for row in records_cursor:
            if self.buffer_size > buffer_limit:
                self._write_batch()

            record = (dict(zip(
                [recordidcolumn, 'title', 'title_fr', 'pub_date', 'series', 'deleted', 'local_identifier',
                 'item_url', 'modified_timestamp',
                 'repository_url', 'repository_name', 'repository_name_fr','repository_thumbnail', 'item_url_pattern',
                 'last_crawl_timestamp'], row)))
            record["deleted"] = int(record["deleted"])

            if record["item_url"] == "" and record["modified_timestamp"] != 0:
                record["item_url"] = self.db.construct_local_url(record)

            if record.get("item_url") is None:
                continue
                
            if record["deleted"] == 1:
                deleted.append(record["item_url"])
                continue

            if ((record["title"] is None or len(record["title"]) == 0) and 
                (record["title_fr"] is None or len(record["title_fr"]) == 0)):
                continue

            con = self.db.getConnection()
            with con:
                litecur = self.db.getDictCursor()

                litecur.execute(self.db._prep("""SELECT geobbox.westLon, geobbox.eastLon, geobbox.northLat, geobbox.southLat
                                    FROM geobbox WHERE geobbox.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                geobboxes = litecur.fetchall()
                if len(geobboxes) > 0:
                    record["datacite_geoLocationBox"] = []
                    for geobbox in geobboxes:
                        record["datacite_geoLocationBox"].append({"westBoundLongitude": float(geobbox["westlon"]),
                                                                  "eastBoundLongitude": float(geobbox["eastlon"]),
                                                                  "northBoundLatitude": float(geobbox["northlat"]),
                                                                  "southBoundLatitude": float(geobbox["southlat"])})


                litecur.execute(self.db._prep("SELECT geopoint.lat, geopoint.lon FROM geopoint WHERE geopoint." + recordidcolumn + "=?"), (record[recordidcolumn],))
                geopoints = litecur.fetchall()
                if len(geopoints) > 0:
                    record["datacite_geoLocationPoint"] = []
                    for geopoint in geopoints:
                        record["datacite_geoLocationPoint"].append({"pointLatitude": float(geopoint["lat"]),
                                                                    "pointLongitude": float(geopoint["lon"])})

                litecur.execute(self.db._prep("""SELECT geoplace.country, geoplace.province_state, geoplace.city, geoplace.other, geoplace.place_name
                    FROM geoplace WHERE geoplace.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                geoplaces = litecur.fetchall()
                if len(geoplaces) > 0:
                    record["datacite_geoLocationPlace"] = []
                    for geoplace in geoplaces:
                        if geoplace["place_name"]:
                            record["datacite_geoLocationPlace"].append({"place_name": geoplace["place_name"]})
                        elif geoplace["country"] or geoplace["province_state"] or geoplace["city"] or geoplace["other"]:
                            record["datacite_geoLocationPlace"].append({"country": geoplace["country"],
                                                                        "province_state": geoplace["province_state"],
                                                                        "city": geoplace["city"],
                                                                        "additional": geoplace["other"]})

                # CRDC (FRDR records only)
                litecur.execute(self.db._prep("""SELECT crdc.crdc_code, crdc.crdc_group_en, crdc.crdc_group_fr, 
                                                    crdc.crdc_class_en, crdc.crdc_class_fr, crdc.crdc_field_en, crdc.crdc_field_fr
                                                    FROM crdc JOIN records_x_crdc on records_x_crdc.crdc_id = crdc.crdc_id
                                                    WHERE records_x_crdc.""" + recordidcolumn + """=?"""),
                                (record[recordidcolumn],))
                crdc_entries = litecur.fetchall()
                if len(crdc_entries) > 0:
                    record["crdc"] = []
                    for crdc_entry in crdc_entries:
                        record["crdc"].append({"crdc_code": crdc_entry["crdc_code"],
                                               "crdc_group_en": crdc_entry["crdc_group_en"], "crdc_group_fr": crdc_entry["crdc_group_fr"],
                                               "crdc_class_en": crdc_entry["crdc_class_en"], "crdc_class_fr": crdc_entry["crdc_class_fr"],
                                               "crdc_field_en": crdc_entry["crdc_field_en"], "crdc_field_fr": crdc_entry["crdc_field_fr"] })

            with con:
                litecur = self.db.getLambdaCursor()

                # attach the other values to the dict
                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.""" + recordidcolumn + """=? AND records_x_creators.is_contributor=0 order by records_x_creators_id asc"""),
                                (record[recordidcolumn],))
                record["dc_contributor_author"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT affiliations.affiliation FROM affiliations JOIN records_x_affiliations on records_x_affiliations.affiliation_id = affiliations.affiliation_id
                    WHERE records_x_affiliations.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                record["datacite_creatorAffiliation"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT creators.creator FROM creators JOIN records_x_creators on records_x_creators.creator_id = creators.creator_id
                    WHERE records_x_creators.""" + recordidcolumn + """=? AND records_x_creators.is_contributor=1 order by records_x_creators_id asc"""),
                                (record[recordidcolumn],))
                record["dc_contributor"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.""" + recordidcolumn + """=? and subjects.language='en'"""), (record[recordidcolumn],))
                record["frdr_subject_en"] = self._rows_to_list(litecur)


                litecur.execute(self.db._prep("""SELECT subjects.subject FROM subjects JOIN records_x_subjects on records_x_subjects.subject_id = subjects.subject_id
                    WHERE records_x_subjects.""" + recordidcolumn + """=? and subjects.language='fr'"""), (record[recordidcolumn],))
                record["frdr_subject_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT publishers.publisher FROM publishers JOIN records_x_publishers on records_x_publishers.publisher_id = publishers.publisher_id
                    WHERE records_x_publishers.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                record["dc_publisher"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT rights.rights FROM rights JOIN records_x_rights on records_x_rights.rights_id = rights.rights_id
                                                       WHERE records_x_rights.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                record["dc_rights"] = self._rows_to_list(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE " + recordidcolumn + "=? and language='en' "),
                    (record[recordidcolumn],))
                record["dc_description_en"] = self._rows_to_list(litecur)

                litecur.execute(
                    self.db._prep("SELECT description FROM descriptions WHERE " + recordidcolumn + "=? and language='fr' "),
                    (record[recordidcolumn],))
                record["dc_description_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.""" + recordidcolumn + """=? and tags.language = 'en' """), (record[recordidcolumn],))
                record["frdr_keyword_en"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT tags.tag FROM tags JOIN records_x_tags on records_x_tags.tag_id = tags.tag_id
                    WHERE records_x_tags.""" + recordidcolumn + """=? and tags.language = 'fr' """), (record[recordidcolumn],))
                record["frdr_keyword_fr"] = self._rows_to_list(litecur)

                litecur.execute(self.db._prep("""SELECT access.access FROM access JOIN records_x_access on records_x_access.access_id = access.access_id
                    WHERE records_x_access.""" + recordidcolumn + """=?"""), (record[recordidcolumn],))
                record["frdr_access"] = self._rows_to_list(litecur)

            with con:
                if self.db.getType() == "sqlite":
                    from sqlite3 import Row
                    con.row_factory = Row
                    litecur = con.cursor()
                elif self.db.getType() == "postgres":
                    litecur = con.cursor(cursor_factory=DictCursor)

                litecur.execute(self.db._prep(
                    """SELECT ds.namespace, dm.field_name, dm.field_value 
                    FROM domain_metadata dm, domain_schemas ds WHERE dm.schema_id=ds.schema_id and dm.""" + recordidcolumn + """=?"""),
                                (record[recordidcolumn],))
                for row2 in litecur:
                    domain_namespace = str(row2["namespace"])
                    field_name = str(row2["field_name"])
                    field_value = str(row2["field_value"])
                    if domain_namespace == "http://datacite.org/schema/kernel-4":
                        custom_label = "datacite_" + field_name
                    else:
                        custom_label = domain_namespace + "#" + field_name
                    if custom_label not in record:
                        record[custom_label] = field_value
                    else:
                        if not isinstance(record[custom_label], list):
                            record[custom_label] = [record[custom_label]]
                        record[custom_label].append(field_value)

            # Check for bilingual domain names
            repo_name = record["repository_name"]
            if (record["repository_name_fr"] is not None and record["repository_name_fr"] != record["repository_name"]):
                repo_name = record["repository_name"] + " / " + record["repository_name_fr"]

            # Convert friendly column names into dc element names
            record["dc_title_en"] = record["title"]
            record["dc_title_fr"] = record["title_fr"]
            record["dc_date"] = record["pub_date"]
            record["frdr_series"] = record["series"]
            record["frdr_origin_id"] = repo_name
            record["frdr_origin_icon"] = record["repository_thumbnail"]
            gmeta_subject = record[recordidcolumn]

            # Concatenate EN and FR into multi-language fields for Globus search
            record["dc_title_multi"] = str(record["dc_title_en"]) + " " + str(record["dc_title_fr"])
            record["dc_title_multi"] = record["dc_title_multi"].strip()
            record["dc_description_multi"] = []
            record["dc_description_multi"].extend(record["dc_description_en"])
            record["dc_description_multi"].extend(record["dc_description_fr"])
            record["frdr_subject_multi"] = []
            record["frdr_subject_multi"].extend(record["frdr_subject_en"])
            record["frdr_subject_multi"].extend(record["frdr_subject_fr"])
            record["frdr_keyword_multi"] = []
            record["frdr_keyword_multi"].extend(record["frdr_keyword_en"])
            record["frdr_keyword_multi"].extend(record["frdr_keyword_fr"])

            # remove unneeded columns from output
            record.pop("contact", None)
            record.pop("deleted", None)
            record.pop("item_url_pattern", None)
            record.pop("last_crawl_timestamp", None)
            record.pop("local_identifier", None)
            record.pop("modified_timestamp", None)
            record.pop("pub_date", None)
            record.pop(recordidcolumn, None)
            record.pop("repository_name", None)
            record.pop("repository_name_fr", None)
            record.pop("repository_thumbnail", None)
            record.pop("repository_url", None)
            record.pop("series", None)
            record.pop("title", None)
            record.pop("title_fr", None)

            record["datacite_resourceTypeGeneral"] = "dataset"
            gmeta_data = {"@datatype": "GMetaEntry", "@version": "2016-11-09",
                          "subject": gmeta_subject, "visible_to": ["public"], "mimetype": "application/json",
                          "content": record}
            self.output_buffer.append(gmeta_data)

            self.buffer_size = self.buffer_size + len(json.dumps(gmeta_data))
            records_assembled += 1
            if (records_assembled % 1000 == 0):
                self.logger.info("Done processing {} records for export".format(records_assembled))

        if self.output_buffer:
            self._write_batch()

        self.logger.info("Export complete: {} items in {} files".format(records_assembled, self.batch_number))
        return deleted


    def change_keys(self, obj, dropkeys, renamekeys):
        """ Recursively goes through the object and replaces keys """
        if self.db.dbtype == "postgres":
            if isinstance(obj, DictRow):
                return obj
        if isinstance(obj, (str, int, float)):
            return obj
        if isinstance(obj, dict):
            new = obj.__class__()
            for k, v in obj.items():
                if k in dropkeys:
                    continue
                strip_dc = re.sub("dc_", "", k)
                if strip_dc in renamekeys:
                    datacite_key = renamekeys[strip_dc]
                else:
                    datacite_key = strip_dc
                newkey = re.sub("[:\.]", "_", datacite_key)
                new[newkey] = self.change_keys(v, dropkeys, renamekeys)
        elif isinstance(obj, (list, set, tuple)):
            new = obj.__class__(self.change_keys(v, dropkeys, renamekeys) for v in obj)
        else:
            return obj
        return new
