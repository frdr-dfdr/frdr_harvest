import os
import time
import hashlib
import json
import re
from psycopg2.extras import DictCursor, RealDictCursor

class DBInterface:
    def __init__(self, params):
        self.dbtype = params.get('type', None)
        self.dbname = params.get('dbname', None)
        self.host = params.get('host', None)
        self.schema = params.get('schema', None)
        self.user = params.get('user', None)
        self.password = params.get('pass', None)
        self.connection = None
        self.logger = None

        if self.dbtype == "sqlite":
            self.dblayer = __import__('sqlite3')
            global Row
            from sqlite3 import Row
            if os.name == "posix":
                try:
                    os.chmod(self.dbname, 0o664)
                except Exception as e:
                    pass

        elif self.dbtype == "postgres":
            self.dblayer = __import__('psycopg2')
            from psycopg2.extras import RealDictCursor

        else:
            raise ValueError('Database type must be sqlite or postgres in config file')

        con = self.getConnection()
        cur = self.getRowCursor()

        # This table must always exist
        cur.execute(
            "create table if not exists "
            "settings (setting_id INTEGER PRIMARY KEY NOT NULL, setting_name TEXT, setting_value TEXT)")

        # Determine if the database schema needs to be updated
        dbversion = int(self.get_setting("dbversion"))
        files = os.listdir("sql/" + str(self.dbtype) + "/")
        files.sort()
        for filename in files:
            if filename.endswith(".sql"):
                scriptversion = int(filename.split('.')[0])
                if scriptversion > dbversion:
                    # Run this script to update the schema, then record it as done
                    with open("sql/" + str(self.dbtype) + "/" + filename, 'r') as scriptfile:
                        scriptcontents = scriptfile.read()
                    if self.dbtype == "postgres":
                        cur.execute(scriptcontents)
                    elif self.dbtype == "sqlite":
                        cur.executescript(scriptcontents)
                    self.set_setting("dbversion", scriptversion)
                    dbversion = scriptversion
                    print("Updated database to version: {:d}".format(scriptversion))  # No logger yet

        self.tabledict = {}
        with open("sql/tables.json", 'r') as jsonfile:
            self.tabledict = json.load(jsonfile)

    def setLogger(self, l):
        self.logger = l

    def getConnection(self):
        if self.connection is None:
            if self.dbtype == "sqlite":
                self.connection = self.dblayer.connect(self.dbname)
            elif self.dbtype == "postgres":
                self.connection = self.dblayer.connect("dbname='%s' user='%s' password='%s' host='%s'" % (
                    self.dbname, self.user, self.password, self.host))
                self.connection.autocommit = True
        return self.connection

    def getDictCursor(self):
        if self.dbtype == "sqlite":
            self.getConnection().row_factory = Row
            cur = self.getConnection().cursor()
        if self.dbtype == "postgres":
            cur = self.getConnection().cursor(cursor_factory=RealDictCursor)
        return cur

    def getRowCursor(self):
        if self.dbtype == "sqlite":
            self.getConnection().row_factory = Row
        cur = self.getConnection().cursor()
        return cur

    def getLambdaCursor(self):
        if self.dbtype == "sqlite":
            self.getConnection().row_factory = lambda cursor, row: row[0]
            cur = self.getConnection().cursor()
        elif self.dbtype == "postgres":
            cur = self.getConnection().cursor(cursor_factory=DictCursor)
        return cur

    def getType(self):
        return self.dbtype

    def _prep(self, statement):
        if (self.dbtype == "postgres"):
            return statement.replace('?', '%s')
        return statement

    def get_setting(self, setting_name):
        # Get an internal setting
        setting_value = 0
        con = self.getConnection()
        res = None
        cur = self.getDictCursor()
        cur.execute(
            self._prep("select setting_value from settings where setting_name = ? order by setting_value desc"),
            (setting_name,))
        if cur is not None:
            res = cur.fetchone()
        if res is not None:
            setting_value = res['setting_value']
        return setting_value

    def set_setting(self, setting_name, new_value):
        curent_value = self.get_setting(setting_name)
        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            if not curent_value:
                cur.execute(self._prep("insert into settings(setting_value, setting_name) values (?,?)"),
                            (new_value, setting_name))
            else:
                cur.execute(self._prep("update settings set setting_value = ? where setting_name = ?"),
                            (new_value, setting_name))

    def update_repo(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            if self.repo_id > 0:
                # Existing repo
                try:
                    self.logger.debug("This repo already exists in the database; updating")
                    cur.execute(self._prep("""UPDATE repositories
                        set repository_url=?, repository_set=?, repository_name=?, repository_type=?,
                        repository_thumbnail=?, last_crawl_timestamp=?, item_url_pattern=?,enabled=?,
                        abort_after_numerrors=?,max_records_updated_per_run=?,update_log_after_numitems=?,
                        record_refresh_days=?,repo_refresh_days=?,homepage_url=?,repo_oai_name=?
                        WHERE repository_id=?"""), (
                        self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail, time.time(),
                        self.item_url_pattern,
                        self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                        self.update_log_after_numitems,
                        self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name, self.repo_id))
                except self.dblayer.IntegrityError as e:
                    # record already present in repo
                    self.logger.error("Integrity error in update {}".format(e))
                    return self.repo_id
            else:
                # Create new repo record
                try:
                    self.logger.debug("This repo does not exist in the database; adding")
                    if self.dbtype == "postgres":
                        cur.execute(self._prep("""INSERT INTO repositories
                            (repository_url, repository_set, repository_name, repository_type, repository_thumbnail,
                            last_crawl_timestamp, item_url_pattern, enabled,
                            abort_after_numerrors,max_records_updated_per_run,update_log_after_numitems,
                            record_refresh_days,repo_refresh_days,homepage_url,repo_oai_name)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING repository_id"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name))
                        self.repo_id = int(cur.fetchone()['repository_id'])

                    if self.dbtype == "sqlite":
                        cur.execute(self._prep("""INSERT INTO repositories
                            (repository_url, repository_set, repository_name, repository_type, repository_thumbnail,
                            last_crawl_timestamp, item_url_pattern, enabled, abort_after_numerrors,
                            max_records_updated_per_run,update_log_after_numitems,record_refresh_days,repo_refresh_days,
                            homepage_url,repo_oai_name)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""), (
                            self.repo_url, self.repo_set, self.repo_name, self.repo_type, self.repo_thumbnail,
                            time.time(), self.item_url_pattern,
                            self.enabled, self.abort_after_numerrors, self.max_records_updated_per_run,
                            self.update_log_after_numitems,
                            self.record_refresh_days, self.repo_refresh_days, self.homepage_url, self.repo_oai_name))
                        self.repo_id = int(cur.lastrowid)

                except self.dblayer.IntegrityError as e:
                    self.logger.error("Cannot add repository: {}".format(e))

        return self.repo_id

    def get_repo_id(self, repo_url, repo_set):
        returnvalue = 0
        extrawhere = ""
        if repo_set is not None:
            extrawhere = "and repository_set='{}'".format(repo_set)
        records = self.get_multiple_records("repositories", "repository_id", "repository_url", repo_url, extrawhere)
        for record in records:
            returnvalue = int(record['repository_id'])
        # If not found, look for insecure version of the url, it may have just changed to https on this pass
        if returnvalue == 0 and repo_url and repo_url.startswith('https:'):
            repo_url = repo_url.replace("https:", "http:")
            records = self.get_multiple_records("repositories", "repository_id", "repository_url", repo_url, extrawhere)
            for record in records:
                returnvalue = int(record['repository_id'])
        return returnvalue

    def get_repo_last_crawl(self, repo_id):
        returnvalue = 0
        if repo_id == 0 or repo_id is None:
            return 0
        records = self.get_multiple_records("repositories", "last_crawl_timestamp", "repository_id", repo_id)
        for record in records:
            returnvalue = int(record['last_crawl_timestamp'])
        self.logger.debug("Last crawl ts for repo_id {} is {}".format(repo_id, returnvalue))
        return returnvalue

    def get_repositories(self):
        records = self.get_multiple_records("repositories", "*", "enabled", "1", "or enabled = 'true'")
        repos = [dict(rec) for rec in records]
        for i in range(len(repos)):
            records = self.get_multiple_records("records", "count(*) as cnt", "repository_id",
                                                repos[i]["repository_id"], "and modified_timestamp!=0 and (title != '' or title_fr != '') and deleted=0")
            for rec in records:
                repos[i]["item_count"] = int(rec["cnt"])
        return repos

    def get_ror_from_affiliation(self, affiliation_string):
        ror_affiliation_matches = self.get_multiple_records("ror_affiliation_matches", "*", "affiliation_string", affiliation_string)
        if len(ror_affiliation_matches) > 0:
            return ror_affiliation_matches[0]
        return ror_affiliation_matches

    def write_ror_affiliation_match(self, affiliation_string, ror_id, score, country):
        ror_affiliation_match_id = self.get_single_record_id("ror_affiliation_matches", affiliation_string)
        if ror_affiliation_match_id is not None:
            self.delete_row_generic("ror_affiliation_matches", affiliation_string, ror_affiliation_match_id)
        extras = {"ror_id": ror_id, "score": score, "country": country, "updated_timestamp": int(time.time())}
        self.insert_related_record("ror_affiliation_matches", affiliation_string, **extras)
        return self.get_ror_from_affiliation(affiliation_string)

    def update_record(self, record_id, fields):
        update_record_sql = "update records set "
        update_cols = []
        update_vals = []
        for key,value in fields.items():
            update_cols.append("{} = ?".format(key))
            update_vals.append(value)
        update_record_sql += ", ".join(update_cols)
        update_record_sql += " where record_id = ?"
        update_vals.append(record_id)

        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            cur.execute(self._prep(update_record_sql),
                        update_vals)

    def update_last_crawl(self, repo_id):
        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            cur.execute(self._prep("update repositories set last_crawl_timestamp = ? where repository_id = ?"),
                        (int(time.time()), repo_id))

    def set_repo_enabled(self, repo_id, enabled):
        cur = self.getRowCursor()
        cur.execute(self._prep("update repositories set enabled = ? where repository_id = ?"),
                    (enabled, repo_id))

    def delete_record(self, record):
        con = self.getConnection()
        if record['record_id'] == 0:
            return False
        with con:
            cur = self.getRowCursor()
            try:
                cur.execute(self._prep("UPDATE records set deleted = 1, modified_timestamp = ?, upstream_modified_timestamp = ? where record_id=?"),
                            (time.time(), time.time(), record['record_id']))
            except Exception as e:
                self.logger.error("Unable to mark as deleted record {}".format(record['local_identifier']))
                return False

            try: # FIXME delete descriptions and other geo metadata
                self.delete_all_related_records("records_x_access", record['record_id'])
                self.delete_all_related_records("records_x_creators", record['record_id'])
                self.delete_all_related_records("records_x_publishers", record['record_id'])
                self.delete_all_related_records("records_x_rights", record['record_id'])
                self.delete_all_related_records("records_x_subjects", record['record_id'])
                self.delete_all_related_records("records_x_affiliations", record['record_id'])
                self.delete_all_related_records("records_x_tags", record['record_id'])
                self.delete_all_related_records("descriptions", record['record_id'])
                self.delete_all_related_records("geospatial", record['record_id'])
                self.delete_all_related_records("domain_metadata", record['record_id'])
            except Exception as e:
                self.logger.error(
                    "Unable to delete related table rows for record {}".format(record['local_identifier']))
                return False

        self.logger.debug("Marked as deleted: record {}".format(record['local_identifier']))
        return True

    def purge_deleted_records(self):
        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            try:
                sqlstring = "DELETE from records where deleted=1"
                cur.execute(sqlstring)
            except Exception as e:
                return False
        return True

    def delete_all_related_records(self, crosstable, record_id, extrawhere=""):
        return self.delete_row_generic(crosstable, "record_id", record_id, extrawhere)

    def delete_one_related_record(self, crosstable, column_value, record_id, extrawhere=""):
        columnname = self.get_table_value_column(crosstable)
        self.delete_row_generic(crosstable, columnname, column_value, "and record_id="+str(record_id) + " " + extrawhere)

    def delete_row_generic(self, tablename, columnname, column_value, extrawhere=""):
        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            try:
                sqlstring = "DELETE from {} where {}=? {}".format(tablename, columnname, extrawhere)
                cur.execute(self._prep(sqlstring), (column_value,))
            except Exception as e:
                return False
        return True

    def update_row_generic(self, tablename, row_id, updates, extrawhere=""):
        idcolumn = self.get_table_id_column(tablename)
        sqlstring = "UPDATE {} set {} where {}=? {}".format(tablename, "=?, ".join(str(k) for k in list(updates.keys())) + "=?", idcolumn, extrawhere)
        values = list(updates.values())
        values.append(row_id)

        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            try:
                cur.execute(self._prep(sqlstring), values )
            except Exception as e:
                return False
            return True

    def get_table_id_column(self, tablename):
        if tablename in self.tabledict and "idcol" in self.tabledict[tablename]:
            return str(self.tabledict[tablename]["idcol"])
        raise ValueError("tables.json missing idcol definition for {}".format(tablename))

    def get_table_value_column(self, tablename):
        if tablename in self.tabledict and "valcol" in self.tabledict[tablename]:
            return str(self.tabledict[tablename]["valcol"])
        raise ValueError("tables.json missing valcol definition for {}".format(tablename))

    def insert_related_record(self, tablename, val, **kwargs):
        valcolumn = self.get_table_value_column(tablename)
        idcolumn = self.get_table_id_column(tablename)
        related_record_id = None
        paramlist = {valcolumn: val}
        for key, value in kwargs.items():
            paramlist[key] = value
        sqlstring = "INSERT INTO {} ({}) VALUES ({})".format(
            tablename, ",".join(str(k) for k in list(paramlist.keys())),
            ",".join(str("?") for k in list(paramlist.keys())))

        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(sqlstring + " RETURNING " + idcolumn), list(paramlist.values()))
                    related_record_id = int(cur.fetchone()[idcolumn])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(sqlstring), list(paramlist.values()))
                    related_record_id = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return related_record_id

    def insert_cross_record(self, crosstable, relatedtable, related_id, record_id, **kwargs):
        cross_table_id = None
        idcolumn = self.get_table_id_column(crosstable)
        relatedidcolumn = self.get_table_id_column(relatedtable)
        paramlist = {"record_id": record_id, relatedidcolumn: related_id}
        for key, value in kwargs.items():
            paramlist[key] = value
        sqlstring = "INSERT INTO {} ({}) VALUES ({})".format(
            crosstable, ",".join(str(k) for k in list(paramlist.keys())),
            ",".join(str("?") for k in list(paramlist.keys())))

        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(sqlstring + " RETURNING " + idcolumn), list(paramlist.values()))
                    cross_table_id = int(cur.fetchone()[idcolumn])
                elif self.dbtype == "sqlite":
                    cur.execute(self._prep(sqlstring), list(paramlist.values()))
                    cross_table_id = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return cross_table_id


    def get_multiple_records(self, tablename, columnlist, given_col, given_val, extrawhere="", **kwargs):
        records = []
        paramlist = {}
        for key, value in kwargs.items():
            paramlist[key] = value
        if len(paramlist) > 0:
            extrawhere = extrawhere + " and " + "=? and ".join(str(k) for k in list(paramlist.keys())) + "=?"
        sqlstring = "select {} from {} where {}=? {}".format(columnlist, tablename, given_col, extrawhere)
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            cur.execute(self._prep(sqlstring), [given_val] + (list(paramlist.values())))
            if cur is not None:
                records = cur.fetchall()
        return records

    def get_records_raw_query(self, sqlstring):
        records = []
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            cur.execute(self._prep(sqlstring))
            if cur is not None:
                records = cur.fetchall()
        return records

    def get_single_record_id(self, tablename, val, extrawhere="", **kwargs):
        returnvalue = None
        idcolumn = self.get_table_id_column(tablename)
        valcolumn = self.get_table_value_column(tablename)
        records = self.get_multiple_records(tablename, idcolumn, valcolumn, val, extrawhere, **kwargs)
        for record in records:
            returnvalue = int(record[idcolumn])
        return returnvalue

    def construct_local_url(self, record):
        oai_id = None
        oai_search = None
        # Check if the local_identifier has already been turned into a url
        if "local_identifier" in record:
            if record["local_identifier"] and record["local_identifier"].startswith(("http","HTTP")):
                return record["local_identifier"]
            # No link found, see if there is an OAI identifier
            oai_search = re.search("oai:(.+):(.+)", record["local_identifier"])
        else:
            oai_search = re.search("oai:(.+):(.+)", record["identifier"])

        if oai_search:
            # Check for OAI format of identifier (oai:domain:id) and extract just the ID
            oai_id = oai_search.group(2)
            # Replace underscores in IDs with colons (SFU Radar)
            oai_id = oai_id.replace("_", ":")

        # If given a pattern then substitute in the item ID and return it
        if "item_url_pattern" in record and record["item_url_pattern"]:
            if oai_id:
                local_url = re.sub("(\%id\%)", oai_id, record["item_url_pattern"])
            else:
                # No OAI ID found, but we still got passed a pattern, so use it with full identifier
                if "local_identifier" in record and record["local_identifier"]:
                    local_url = re.sub("(\%id\%)", record["local_identifier"], record["item_url_pattern"])
                else:
                    local_url = re.sub("(\%id\%)", record["identifier"], record["item_url_pattern"])
            return local_url

        # Check if the identifier is a DOI
        if "local_identifier" in record and record["local_identifier"]:
            doi = re.search("(doi|DOI):\s?\S+", record["local_identifier"])
            if doi:
                doi = doi.group(0).rstrip('\.')
                local_url = re.sub("(doi|DOI):\s?", "https://doi.org/", doi)
                return local_url

        # Check if the source is already a link
        if "source_url" in record:
            if record["source_url"] and record["source_url"].startswith(("http","HTTP")):
                return record["source_url"]
        if "dc:source" in record:
            if isinstance(record["dc:source"], list):
                if record["dc:source"][0] and record["dc:source"][0].startswith(("http","HTTP")):
                        return record["dc:source"][0]
            else:
                if record["dc:source"] and record["dc:source"].startswith(("http","HTTP")):
                    return record["dc:source"]

        # URL is in the identifier
        if "local_identifier" in record:
            local_url = re.search("(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?",
                                  record["local_identifier"])
            if local_url:
                return local_url.group(0)

        self.logger.error("construct_local_url() failed for item: {}".format(json.dumps(record)) )
        return None

    def create_new_record(self, rec, source_url, repo_id):
        returnvalue = None
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            try:
                if self.dbtype == "postgres":
                    cur.execute(self._prep(
                        """INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, source_url,
                        deleted, local_identifier, item_url, repository_id, upstream_modified_timestamp)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?) RETURNING record_id"""),
                        (rec["title"], rec["title_fr"], rec["pub_date"],  rec["series"], time.time(), source_url, 0,
                         rec["identifier"], rec["item_url"], repo_id, time.time()))
                    returnvalue = int(cur.fetchone()['record_id'])
                if self.dbtype == "sqlite":
                    cur.execute(self._prep(
                        """INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, source_url,
                        deleted, local_identifier, item_url, repository_id, upstream_modified_timestamp)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)"""),
                        (rec["title"], rec["title_fr"], rec["pub_date"], rec["series"], time.time(), source_url, 0,
                         rec["identifier"], rec["item_url"], repo_id, time.time()))
                    returnvalue = int(cur.lastrowid)
            except self.dblayer.IntegrityError as e:
                self.logger.error("Record insertion problem: {}".format(e))

        return returnvalue

    def update_metadata(self, record, crosstable, valtable, val_fieldname, val_idcol, extrawhere="", extras={}):
        modified_upstream = False
        if val_fieldname in ["tags", "subject"]:
            existing_val_recs = self.get_records_raw_query("""select v.{} from {} v
                               join {} x on x.{} = v.{}
                               where x.record_id = {} and v.language = 'en' """.format(val_idcol, valtable, crosstable, val_idcol, val_idcol, record["record_id"]))
        elif val_fieldname in ["tags_fr", "subject_fr"]:
            existing_val_recs = self.get_records_raw_query("""select v.{} from {} v
                               join {} x on x.{} = v.{}
                               where x.record_id = {} and v.language = 'fr' """.format(val_idcol, valtable, crosstable, val_idcol, val_idcol, record["record_id"]))
        else:
            existing_val_recs = self.get_multiple_records(crosstable, val_idcol, "record_id",
                                                      record["record_id"], extrawhere)
        existing_val_recs_ids = [e[val_idcol] for e in existing_val_recs]
        if val_fieldname in record:
            if not isinstance(record[val_fieldname], list):
                record[val_fieldname] = [record[val_fieldname]]
            new_val_recs_ids = []
            for value in record[val_fieldname]:
                # special cases
                if val_fieldname == "geoplaces":
                    if "country" not in value:
                        value["country"] = ""
                    if "province_state" not in value:
                        value["province_state"] = ""
                    if "city" not in value:
                        value["city"] = ""
                    if "other" not in value:
                        value["other"] = ""
                    if "place_name" not in value:
                        value["place_name"] = ""
                    extras = {"country": value["country"], "province_state": value["province_state"], "city": value["city"], "other": value["other"]}
                    value = value["place_name"]
                elif val_fieldname == "geopoints":
                    if "lat" in value and "lon" in value:
                        try:
                            value["lat"] = float(value["lat"])
                            value["lon"] = float(value["lon"])
                            # Check coordinates are valid numbers
                            if not (self.check_lat(value.get("lat")) and self.check_long(value.get("lon"))):
                                continue
                            extras = {"lat": value["lat"], "lon": value["lon"]}
                        except Exception as e:
                            self.logger.error("Unable to update geopoint for record id {}: {}".format(record['record_id'], e))
                            continue
                elif val_fieldname == "geobboxes":
                    try:
                        # Fill in any missing values
                        if "eastLon" not in value and "westLon" in value:
                            value["eastLon"] = value["westLon"]
                        if "westLon" not in value and "eastLon" in value:
                            value["westLon"] = value["eastLon"]
                        if "northLat" not in value and "southLat" in value:
                            value["northLat"] = value["southLat"]
                        if "southLat" not in value and "northLat" in value:
                            value["southLat"] = value["northLat"]
                        # Check all coordinates are valid numbers
                        if not (self.check_lat(value.get("northLat")) and self.check_lat(value.get("southLat")) and
                                self.check_long(value.get("westLon")) and self.check_long(value.get("eastLon"))):
                            continue
                        if value["westLon"] != value["eastLon"] or value["northLat"] != value["southLat"]:
                            # If west/east or north/south don't match, this is a box
                            extras = {"westLon": value["westLon"], "eastLon": value["eastLon"],
                                      "northLat": value["northLat"], "southLat": value["southLat"]}
                        else: # TODO verify this appends to the original record dictionary?
                            if "geopoints" not in record:
                                record["geopoints"] = []
                            record["geopoints"].append({"lat": value["northLat"], "lon": value["westLon"]})
                            continue
                    except Exception as e:
                        self.logger.error("Unable to update geobbox for record id {}: {}".format(record['record_id'], e))
                        continue
                elif val_fieldname == "geofiles":
                    if "filename" in value and "uri" in value:
                        extras = {"filename": value["filename"], "uri": value["uri"]}
                elif val_fieldname == "affiliation": # TODO test for Dryad
                    if isinstance(value, dict) and "affilation_ror" in value:
                        extras = {"affiliation_ror": value["affiliation_ror"]}
                    else:
                        extras = {"affiliation_ror": ""}
                    if isinstance(value, dict) and "affiliation_name" in value:
                        value = value["affiliation_name"]
                elif val_fieldname in ["rights", "description", "description_fr"]:
                    sha1 = hashlib.sha1()
                    sha1.update(value.encode('utf-8'))
                    original_value = value
                    value = sha1.hexdigest()
                    if val_fieldname == "rights":
                        extras = {"rights": original_value}
                    elif val_fieldname == "description":
                        extras =  {"record_id": record["record_id"], "language": "en"}
                    elif val_fieldname == "description_fr":
                        extras = {"record_id": record["record_id"], "language": "fr"}

                # get existing value record if it exists
                if val_fieldname in ["affiliation", "description", "description_fr", "geoplaces"]:
                    val_rec_id = self.get_single_record_id(valtable, value, **extras)
                elif val_fieldname in ["tags", "tags_fr", "subject", "subject_fr"]:
                    val_rec_id = self.get_single_record_id(valtable, value, extrawhere)
                elif val_fieldname in ["geopoints", "geobboxes", "geofiles"]:
                    val_rec_id = self.get_single_record_id(valtable, record["record_id"], **extras)
                else: # ["creator", "contributor", "publisher", "rights"]
                    val_rec_id = self.get_single_record_id(valtable, value)

                # write valtable record and crosstable records if needed
                if val_rec_id is None:
                    if val_fieldname in ["description", "description_fr"]:
                        extras["description"] = original_value

                    if val_fieldname in ["creator", "contributor", "publisher"]:
                        val_rec_id = self.insert_related_record(valtable, value)
                    elif val_fieldname in ["geopoints", "geobboxes", "geofiles"]:
                        val_rec_id = self.insert_related_record(valtable, record["record_id"], **extras)
                    else: # ["affiliation", "rights", "tags", "tags_fr", "subject", "subject_fr", "description", "description_fr", "geoplaces"]:
                        val_rec_id = self.insert_related_record(valtable, value, **extras)
                    if val_fieldname != "geopoints": # Remove when Geodisy starts processing points
                        modified_upstream = True
                if val_rec_id is not None:
                    new_val_recs_ids.append(val_rec_id)
                    if crosstable != valtable:
                        if val_rec_id not in existing_val_recs_ids:
                            if val_fieldname in ["creator", "contributor"]:
                                self.insert_cross_record(crosstable, valtable, val_rec_id, record["record_id"],
                                                         **extras)
                            else:
                                self.insert_cross_record(crosstable, valtable, val_rec_id, record["record_id"])
                        modified_upstream = True
            for eid in existing_val_recs_ids:
                if eid not in new_val_recs_ids:
                    modified_upstream = True
                    if crosstable != valtable:
                        self.delete_one_related_record(crosstable, eid, record["record_id"], extrawhere)
                    else:
                        self.delete_row_generic(valtable, val_idcol, eid)
        elif existing_val_recs: # delete metadata if the field is no longer present in incoming record
            modified_upstream = True
            if crosstable != valtable:
                if val_fieldname in ["subject", "subjects_fr", "tags", "tags_fr"]:
                    for eid in existing_val_recs_ids:
                        self.delete_one_related_record(crosstable, eid, record["record_id"])
                else: # TODO test for "description", "description_fr"
                    self.delete_all_related_records(crosstable, record["record_id"], extrawhere)
            else:
                for eid in existing_val_recs_ids:
                    self.delete_row_generic(valtable, val_idcol, eid)
        return modified_upstream

    def write_record(self, record, repo):
        repo_id = repo.repository_id
        domain_metadata = repo.domain_metadata
        modified_upstream = False # Track whether metadata changed since last crawl

        if record is None:
            return None
        record["record_id"] = self.get_single_record_id("records", record["identifier"],
                                                        "and repository_id=" + str(repo_id))
        record["item_url_pattern"] = repo.item_url_pattern
        if record.get("item_url", None) is None:
            record["item_url"] = self.construct_local_url(record)

        con = self.getConnection()
        with con:
            cur = self.getRowCursor()
            source_url = ""
            if 'dc:source' in record:
                if isinstance(record["dc:source"], list):
                    source_url = record["dc:source"][0]
                else:
                    source_url = record["dc:source"]
            if record["record_id"] is None:
                modified_upstream = True # New record has new metadata
                record["record_id"] = self.create_new_record(record, source_url, repo_id)
            else:
                # Compare title, title_fr, pub_date, series, source_url, item_url, local_identifier for changes
                records = self.get_multiple_records("records", "*", "record_id", record["record_id"])
                if len(records) == 1:
                    existing_record = records[0]
                    if existing_record["title"] != record["title"]:
                        modified_upstream = True
                    elif existing_record["title_fr"] != record["title_fr"]:
                        modified_upstream = True
                    elif existing_record["pub_date"] != record["pub_date"]:
                        modified_upstream = True
                    elif existing_record["series"] != record["series"]:
                        modified_upstream = True
                    elif existing_record["source_url"] is None and existing_record["source_url"] != source_url:
                        modified_upstream = True
                    elif existing_record["item_url"] != record["item_url"]:
                        modified_upstream = True
                    elif existing_record["local_identifier"] != record["identifier"]:
                        modified_upstream = True
                cur.execute(self._prep(
                    """UPDATE records set title=?, title_fr=?, pub_date=?, series=?, modified_timestamp=?, source_url=?,
                    deleted=?, local_identifier=?, item_url=?
                    WHERE record_id = ?"""),
                    (record["title"], record["title_fr"], record["pub_date"], record["series"], time.time(),
                     source_url, 0, record["identifier"], record["item_url"], record["record_id"]))

            if record["record_id"] is None:
                return None

            # creators
            if self.update_metadata(record, "records_x_creators", "creators", "creator",  "creator_id", "and is_contributor=0", {"is_contributor": 0}):
                modified_upstream = True

            # contributors
            if self.update_metadata(record, "records_x_creators", "creators", "contributor", "creator_id", "and is_contributor=1", {"is_contributor": 1}):
                modified_upstream = True

            # publishers
            if self.update_metadata(record, "records_x_publishers", "publishers", "publisher", "publisher_id"):
                modified_upstream = True

            # affiliations
            if self.update_metadata(record, "records_x_affiliations", "affiliations", "affiliation", "affiliation_id"):
                modified_upstream = True

            # access
            if self.update_metadata(record, "records_x_access", "access", "access", "access_id"):
                modified_upstream = True

            # rights
            if self.update_metadata(record, "records_x_rights", "rights", "rights", "rights_id"):
                modified_upstream = True

            # tags - en
            if self.update_metadata(record, "records_x_tags", "tags", "tags", "tag_id", "and language='en'", {"language": "en"}):
                modified_upstream = True

            # tags - fr
            if self.update_metadata(record, "records_x_tags", "tags", "tags_fr", "tag_id", "and language='fr'", {"language": "fr"}):
                modified_upstream = True

            # subjects - en
            if self.update_metadata(record, "records_x_subjects", "subjects", "subject", "subject_id", "and language='en'", {"language": "en"}):
                modified_upstream = True

            # subjects - fr
            if self.update_metadata(record, "records_x_subjects", "subjects", "subject_fr", "subject_id", "and language='fr'", {"language": "fr"}):
                modified_upstream = True

            # geoplaces
            if self.update_metadata(record, "records_x_geoplace", "geoplace", "geoplaces", "geoplace_id"):
                modified_upstream = True

            # descriptions - en
            if self.update_metadata(record, "descriptions", "descriptions", "description", "description_id", "and language='en'"):
                modified_upstream = True

            # descriptions - fr
            if self.update_metadata(record, "descriptions", "descriptions", "description_fr", "description_id", "and language='fr'"):
                modified_upstream = True

            # geobboxes
            if self.update_metadata(record, "geobbox", "geobbox", "geobboxes", "geobbox_id"):
                modified_upstream = True

            # geopoints
            if self.update_metadata(record, "geopoint", "geopoint", "geopoints", "geopoint_id"):
                modified_upstream = True

            # geofiles
            if self.update_metadata(record, "geofile", "geofile", "geofiles", "geofile_id"):
                modified_upstream = True

            # crdc
            if "crdc" in record:
                existing_crdc_recs = self.get_multiple_records("records_x_crdc", "*", "record_id",
                                                                   record["record_id"])
                existing_crdc_ids = [e["crdc_id"] for e in existing_crdc_recs]
                new_crdc_ids = []
                crdc_key_list = ["crdc_code", "crdc_group_en", "crdc_group_fr", "crdc_class_en", "crdc_class_fr", "crdc_field_en", "crdc_field_fr"]
                for crdc in record["crdc"]:
                    for key in crdc_key_list:
                        if key not in crdc:
                            continue # FIXME not sure this breaks loop to the correct level
                    crdc_id = self.get_single_record_id("crdc", crdc["crdc_code"])
                    extras = crdc.copy()
                    extras.pop("crdc_code")
                    if crdc_id is not None:
                        # check if the existing CRDC entry matches - if not, update
                        crdc_record = self.get_multiple_records("crdc", "*", "crdc_id", crdc_id)[0]
                        for key in crdc_key_list:
                            if crdc[key] != crdc_record[key]:
                                self.update_row_generic("crdc", crdc_id, extras)
                                modified_upstream = True
                                break
                    if crdc_id is None:
                        crdc_id = self.insert_related_record("crdc", crdc["crdc_code"], **extras)
                        modified_upstream = True
                    if crdc_id is not None:
                        new_crdc_ids.append(crdc_id)
                        if crdc_id not in existing_crdc_ids:
                            self.insert_cross_record("records_x_crdc", "crdc", crdc_id, record["record_id"])
                            modified_upstream = True

                for eid in existing_crdc_ids:
                    if eid not in new_crdc_ids:
                        records_x_crdc_id = \
                            self.get_multiple_records("records_x_crdc", "records_x_crdc_id", "record_id",
                                                      record["record_id"], " and crdc_id='"
                                                      + str(eid) + "'")[0]["records_x_crdc_id"]
                        self.delete_row_generic("records_x_crdc", "records_x_crdc_id", records_x_crdc_id)
                        modified_upstream = True

            # domain metadata
            if len(domain_metadata) > 0:
                existing_metadata_recs = self.get_multiple_records("domain_metadata", "*", "record_id",
                                                                  record["record_id"])
                existing_metadata_ids = [e["metadata_id"] for e in existing_metadata_recs]
                new_metadata_ids = []
                for field_uri in domain_metadata:
                    field_pieces = field_uri.split("#")
                    domain_schema = field_pieces[0]
                    field_name = field_pieces[1]
                    schema_id = self.get_single_record_id("domain_schemas", domain_schema)
                    if schema_id is None:
                        schema_id = self.insert_related_record("domain_schemas", domain_schema)
                    if not isinstance(domain_metadata[field_uri], list):
                        domain_metadata[field_uri] = [domain_metadata[field_uri]]
                    for field_value in domain_metadata[field_uri]:
                        extras = {"record_id": record["record_id"], "field_name": field_name, "field_value": field_value}
                        metadata_id = self.get_single_record_id("domain_metadata", schema_id, "", **extras)
                        if metadata_id is None:
                            extras = {"record_id": record["record_id"], "field_name": field_name,
                                      "field_value": field_value}
                            metadata_id = self.insert_related_record("domain_metadata", schema_id, **extras)
                        if metadata_id is not None:
                            new_metadata_ids.append(metadata_id)

                for eid in existing_metadata_ids:
                    if eid not in new_metadata_ids:
                        self.delete_row_generic("domain_metadata", "metadata_id", eid)
                        modified_upstream = True

            if modified_upstream:
                self.update_record_upstream_modified(record)

        return None

    def get_stale_records(self, stale_timestamp, repo_id, max_records_updated_per_run):
        con = self.getConnection()
        records = []
        with con:
            cur = self.getDictCursor()
            cur.execute(self._prep("""SELECT recs.record_id, recs.title, recs.pub_date, recs.series
                , recs.modified_timestamp, recs.local_identifier, recs.item_url
                , repos.repository_id, repos.repository_type, recs.geodisy_harvested
                FROM records recs, repositories repos
                where recs.repository_id = repos.repository_id and recs.modified_timestamp < ?
                and repos.repository_id = ? and recs.deleted = 0
                LIMIT ?"""), (stale_timestamp, repo_id, max_records_updated_per_run))
            if cur is not None:
                records = cur.fetchall()

        return records

    def touch_record(self, record):
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            try:
                cur.execute(self._prep("UPDATE records set modified_timestamp = ? where record_id = ?"),
                            (time.time(), record['record_id']))
            except Exception as e:
                self.logger.error("Unable to update modified_timestamp for record id {}".format(record['record_id']))
                return False

        return True

    def write_header(self, local_identifier, repo_id):
        record_id = self.get_single_record_id("records", local_identifier, "and repository_id=" + str(repo_id))
        if record_id is None:
            con = self.getConnection()
            with con:
                cur = self.getDictCursor()
                try:
                    cur.execute(self._prep(
                        "INSERT INTO records (title, title_fr, pub_date, series, modified_timestamp, local_identifier"
                        ", item_url, repository_id, upstream_modified_timestamp) VALUES(?,?,?,?,?,?,?,?,?)"),
                        ("", "", "", "", 0, local_identifier, "", repo_id, time.time()))
                except self.dblayer.IntegrityError as e:
                    self.logger.error("Error creating record header: {}".format(e))

        return None

    def update_record_upstream_modified(self, record):
        con = self.getConnection()
        with con:
            cur = self.getDictCursor()
            try:
                cur.execute(self._prep("UPDATE records set upstream_modified_timestamp = ?, geodisy_harvested = 0 where record_id = ?")
                            , (time.time(), record['record_id']))
            except self.dblayer.IntegrityError as e:
                self.logger.error("Unable to update modified_timestamp for record id ? dur to error creating"
                                  " record header: ?", record['record_id'], e)

        return None

    def check_lat(self,lat):
        if isinstance(lat, str):
            lat = float(lat)
        if lat > 90 or lat < -90:
            return False
        return True

    def check_long(self,long):
        if isinstance(long, str):
            long = float(long)
        if long > 180 or long < -180:
            return False
        return True
