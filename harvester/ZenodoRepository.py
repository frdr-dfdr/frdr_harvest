from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
import re
import os.path
from dateutil import parser


class ZenodoRepository(HarvestRepository):
    """ Zenodo Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "zenodo"
        self.default_language = "en"
        super(ZenodoRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {'accept': 'application/vnd.api+json'}
        self.ror_base_url = "https://api.ror.org/organizations"
        self.ror_ids_countries = {}
        self.headers = {
            'accept': "application/json",
            "content-type": "application/json"
        }

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "zenodo",
            "enabled": self.enabled, "repo_thumbnail": self.thumbnail, "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days, "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            # Initial API call
            url = self.url + "/records/?type=dataset&size=100"
            r = requests.request("GET", url, headers=self.headers)
            response = r.json()
            records = response["hits"]["hits"]

            item_count = 0
            total_zenodo_dataset_count = response['hits']['total'] # hardcode 1000 for testing

            while item_count < total_zenodo_dataset_count:
                for record in records:
                    if "doi" in record and record["doi"]:
                        item_identifier = record["doi"]
                        result = self.db.write_header(item_identifier, self.repository_id)
                        item_count = item_count + 1
                        if (item_count % self.update_log_after_numitems == 0):
                            tdelta = time.time() - self.tstart + 0.1
                            self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                                           self.formatter.humanize(
                                                                                                               tdelta),
                                                                                                           item_count / tdelta))
                if "next" in response["links"]:
                    url = self.url + response["links"]["next"]
                    r = requests.request("GET", url, headers=self.headers)
                    response = r.json()
                    records = response["hits"]["hits"]
                else:
                    break

            self.logger.info("Found {} items in feed".format(item_count))
            return True

        except Exception as e:
            self.logger.error("Updating Zenodo Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True
        return False

    def format_zenodo_to_oai(self, zenodo_record):
        record = {}
        record["identifier"] = zenodo_record["doi"]
        record["item_url"] = "https://doi.org/" + zenodo_record["doi"]

        is_canadian = False
        for creator in zenodo_record["metadata"]["creators"]:
            if "affiliation" in creator and creator["affiliation"]:
                url = self.ror_base_url + "?affiliation=" + creator['affiliation']
                r = requests.request("GET", url, headers=headers)
                ror_records = r.json()
                if len(ror_records["items"]) > 0:
                    ror_best_match = ror_records["items"][0]
                    if ror_best_match["score"] > 0.8 and ror_best_match["organization"]["country"]["country_code"] == "CA":
                        is_canadian = True
                        break
        if not is_canadian:
            return False

        record["creator"] = []
        record["affiliation"] = []
        for creator in zenodo_record["metadata"]["creators"]:
            if "name" in creator and creator["name"]:
                record["creator"].append(creator["name"])
            if "affiliation" in creator and creator["affiliation"]:
                record["affiliation"].append(creator["affiliation"])
        if len(record["affiliation"]) == 0:
            record.pop("affiliation")

        record["title"] = zenodo_record["metadata"]["title"]
        record["title_fr"] = ""
        record["series"] = ""
        record["pub_date"] = zenodo_record["metadata"]["publication_date"]
        record["description"] = zenodo_record["metadata"]["description"]
        record["rights"] = zenodo_record["metadata"]["license"]["id"]

        if "keywords" in zenodo_record["metadata"] and zenodo_record["metadata"]["keywords"]:
            record["tags"] = zenodo_record["metadata"]["keywords"]

        if "locations" in zenodo_record["metadata"] and zenodo_record["metadata"]["locations"]:
            record["geoplaces"] = []
            for location in zenodo_record["metadata"]["locations"]:
                record["geoplaces"].append({"place_name": location["place"]})

        if "access_right" in zenodo_record["metadata"] and zenodo_record["metadata"]["access_right"]:
            if zenodo_record["metadata"]["access_right"] == "closed":
                record["access"] = "Closed"
            elif zenodo_record["metadata"]["access_right"] == "restricted":
                record["access"] = "Restricted"
            elif zenodo_record["metadata"]["access_right"] == "embargoed":
                record["access"] = "Embargoed"
            else: # includes access_right "open"
                record["access"] = "Public"

        record["geofiles"] = []
        for file in zenodo_record["files"]:
            if file["type"].lower() in self.geofile_extensions:
                geofile = {}
                geofile["filename"] = file["key"]
                geofile["uri"] = file["links"]["self"]
                record["geofiles"].append(geofile)

        return record

    def _update_record(self, record):
        try:
            record_url = self.url + "/records/?q=doi:\"" + record["local_identifier"] + "\""
            try:
                item_response = requests.get(record_url)
                zenodo_record = json.loads(item_response.text)["hits"]["hits"][0]
            except Exception as e:
                # Exception means this URL was not found
                self.logger.error("Fetching record {} failed: {}".format(record_url, e))
                return True
            oai_record = self.format_zenodo_to_oai(zenodo_record)
            if oai_record:
                self.db.write_record(oai_record, self)
            else:
                if oai_record is False:
                    # This dataset is not Canadian, remove it from the results
                    self.db.delete_record(record)
                else:
                    # Some other problem, this record will be updated by a future crawl
                    self.db.touch_record(record)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {}".format(record['local_identifier'], e))
            if self.dump_on_failure == True:
                try:
                    print(zenodo_record)
                except:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

