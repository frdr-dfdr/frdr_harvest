from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
from dateutil import parser
import time
import json
import requests


class NexusRepository(HarvestRepository):
    """ Nexus Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "nexus"
        super(NexusRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {'accept': 'application/ld+json'}

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "nexus",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name,
            "repo_registry_uri": self.repo_registry_uri
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            item_count = 0
            query_url = self.url
            while query_url:
                querystring = {"type": "https://schema.org/Dataset"}
                response = requests.request("GET", query_url, headers=self.headers, params=querystring)
                response = response.json()

                for record in response["_results"]:
                    item_identifier = record["_self"]
                    self.db.write_header(item_identifier, self.item_url_pattern, self.repository_id)
                    item_count = item_count + 1
                    if (item_count % self.update_log_after_numitems == 0):
                        tdelta = time.time() - self.tstart + 0.1
                        self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                                   self.formatter.humanize(
                                                                                                       tdelta),
                                                                                                   item_count / tdelta))

                if "_next" in response:
                    query_url = response["_next"]
                else:
                    break
            self.logger.info("Found {} items in feed".format(item_count))

            return True

        except Exception as e:
            self.logger.error("Updating Nexus Repository failed: {} {}".format(type(e).__name__, e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_nexus_to_oai(self, nexus_dats_json):

        nexus_record = nexus_dats_json

        # TODO re-check on full run
        # for key in nexus_record.keys():
        #     print("{}||{}".format(key, type(nexus_record[key])))

        record = {}

        record["series"] = ""
        record["title_fr"] = ""

        if ("_self" in nexus_record) and nexus_record["_self"]:
            record["identifier"] = nexus_dats_json["_self"]

        # Item URL
        if ("conp_portal_website" in nexus_record) and nexus_record["conp_portal_website"]:
            if ("@id" in nexus_record["conp_portal_website"]) and nexus_record["conp_portal_website"]["@id"]:
                record["item_url"] = nexus_record["conp_portal_website"]["@id"]

        if "item_url" not in record: # TODO investigate items missing URLs
            self.logger.error("Record {} missing item_url".format(record["identifier"]))
            return None

        # Description
        if ("description" in nexus_record) and nexus_record["description"]:
            record["description"] = nexus_record["description"]

        # Subjects
        if ("isAbout" in nexus_record) and nexus_record["isAbout"]:
            if isinstance(nexus_record["isAbout"], list):
                isAbout_list = nexus_record["isAbout"]
            else:
                isAbout_list = [nexus_record["isAbout"]]
            record["subjects"] = []
            for subject in isAbout_list:
                if ("name" in subject) and subject["name"]:
                    record["subjects"].append(subject["name"])

        # Rights
        if ("licenses" in nexus_record) and nexus_record["licenses"]:
            if ("name" in nexus_record["licenses"]) and nexus_record["licenses"]["name"]:
                record["rights"] = nexus_record["licenses"]["name"]

        # Title
        if ("name" in nexus_record) and nexus_record["name"]:
            record["title"] = nexus_record["name"]
            if isinstance(record["title"], list):
                if len(set(record["title"]) == 1):
                    record["title"] = record["title"][0]
                else:
                    self.logger.error("Record {} has multiple titles: {}".format(record["identifier"], record["title"]))
                    return None

        # Access
        if ("privacy" in nexus_record) and nexus_record["privacy"]:
            if isinstance(nexus_record["privacy"], str):
                if nexus_record["privacy"].lower() in ["open", "public", "public open"]:
                    record["access"] = "Public"
                elif nexus_record["privacy"].lower() in ["private", "registered access", "registered"]:
                    record["access"] = "Restricted"
                else:
                    record["access"] = "Unknown"
            elif isinstance(nexus_record["privacy"], list):
                if "available" in nexus_record["privacy"]:
                    if "open" in nexus_record["privacy"]:
                        record["access"] = "Public"
                    elif "private" in nexus_record["privacy"] or "registered" in nexus_record["privacy"]:
                        record["access"] = "Restricted"
                    else:
                        record["access"] = "Unknown"
                else:
                    record["access"] = "Unknown"
            else:
                record["access"] = "Unknown"

        # TODO: "acknowledges", "citations", "primaryPublications" for related publications

        if ("sdo:creator" in nexus_record) and nexus_record["sdo:creator"]:
            if isinstance(nexus_record["sdo:creator"], list):
                creators_list = nexus_record["sdo:creator"]
            else:
                creators_list = [nexus_record["sdo:creator"]]
            record["creator"] = []
            for creator in creators_list:
                if ("name" in creator) and creator["name"]:
                        record["creator"] = creator["name"]

        # Keywords
        record["tags"] = []
        if ("sdo:keywords" in nexus_record) and nexus_record["sdo:keywords"]:
            if isinstance(nexus_record["sdo:keywords"], list):
                keywords_list = nexus_record["sdo:keywords"]
            else:
                keywords_list = [nexus_record["sdo:keywords"]]
            for keyword in keywords_list:
                if ("value" in keyword) and keyword["value"]:
                        record["tags"].append(keyword["value"])
        if ("keywords" in nexus_record) and nexus_record["keywords"]:
            if isinstance(nexus_record["keywords"], list):
                keywords_list = nexus_record["keywords"]
            else:
                keywords_list = [nexus_record["keywords"]]
            for keyword in keywords_list:
                if ("value" in keyword) and keyword["value"]:
                        record["tags"].append(keyword["value"])

        if ("_createdAt" in nexus_record) and nexus_record["_createdAt"]:
            record["pub_date"] = parser.parse(nexus_record["_createdAt"]).strftime('%Y-%m-%d')

        # Prefer more specific dates if available
        if ("dates" in nexus_record) and nexus_record["dates"]:
            if isinstance(nexus_record["dates"], list):
                dates_list = nexus_record["dates"]
            else:
                dates_list = [nexus_record["dates"]]
            for date_entry in dates_list:
                date = ""
                date_type = ""
                if ("date" in date_entry) and date_entry["date"]:
                    date_date = date_entry["date"]
                    if isinstance(date_date, list) and len(date_date) == 2 and ("value" in date_date[1]):
                        date = date_date[0]
                        date_type = date_date[1]["value"]
                    elif isinstance(date_date, str) and "type" in date_entry:
                        date = date_date
                        date_type = date_entry["type"]
                    else:
                        print("invalid date")
                    date_type = date_type.lower().strip()

                    try:
                        parser.parse(date).strftime('%Y-%m-%d')
                        if date_type in ["date created", "release date", "first published", "publication date"]:
                            record["pub_date"] = date
                        elif date_type in ["date modified", "last update date", "conp dats json fileset creation date"]:
                            if date < record["pub_date"]:
                                record["pub_date"] = date
                        elif date_type not in ["start date", "end date", "reference data download date", "first data collection", "last data collection", "this dataset was published in june 2019 in the journal of federation of american societies for experimental biology"]:
                            print("unknown type - date: {} type: {}".format(date, date_type))
                    except:
                        self.logger.error("Record {} failed to parse date: {}".format(record["identifier"], date))
                else:
                    self.logger.error("Record {} has invalid date entry: {}".format(record["identifier"], date_entry))
        if "pub_date" not in record:
            self.logger.error("Record {} missing pub_date".format(record["identifier"]))
            return None

        # Geographic places
        if ("spatialCoverage" in nexus_record) and nexus_record["spatialCoverage"]:
            record["geoplaces"] = []
            if isinstance(nexus_record["spatialCoverage"], list):
                places_list = nexus_record["spatialCoverage"]
            else:
                places_list = [nexus_record["spatialCoverage"]]
            for place in places_list:
                if ("name" in place) and place["name"]:
                    record["geoplaces"].append({"place_name": place["name"]})
        return record

    @rate_limited(5)
    def _update_record(self, record):
        try:
            identifier = record['local_identifier']
            try:
                item_response = requests.request("GET", identifier, headers=self.headers)
                item_response = item_response.json()
            except Exception as e:
                # Exception means this URL was not found
                self.db.delete_record(record)
                return True

            oai_record = self.format_nexus_to_oai(item_response)
            if oai_record:
                self.db.write_record(oai_record, self)
            else:
                if oai_record is False:
                    # This record is not a dataset, remove it from the results
                    self.db.delete_record(record)
                else:
                    # Some other problem, this record will be updated by a future crawl
                    self.db.touch_record(record)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {} {}".format(record['local_identifier'], type(e).__name__, e))
            if self.dump_on_failure == True:
                try:
                    print(item_response)
                except Exception as e:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False
    