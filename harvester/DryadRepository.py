from harvester.HarvestRepository import HarvestRepository
import requests
import time
import json
from datetime import datetime
import urllib


class DryadRepository(HarvestRepository):
    """ Dryad Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "dryad"
        self.default_language = "en"
        super(DryadRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {
            'accept': "application/json",
            "content-type": "application/json"
        }
        self.ror_data = None

    def _crawl(self):
        if not self.load_ror_data():
            self.logger.error("ROR data could not be fetched from remote")
            return

        if not self.ror_data:
            self.logger.error("ROR data could not be loaded from the local JSON file")
            return

        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "dryad",
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
            url = self.url + "/search/"
            # Check for records updated in the past 30 days
            querystring = {"per_page": str(100), "modifiedSince": datetime.strftime(datetime.fromtimestamp(self.last_crawl - 60*60*24*7), '%Y-%m-%dT%H:%M:%SZ')}
            r = requests.request("GET", url, headers=self.headers, params=querystring)
            response = r.json()
            records = response['_embedded']['stash:datasets']

            item_count = 0
            total_dryad_item_count = response['total'] # hardcode 1000 for testing

            while item_count < total_dryad_item_count:
                for record in records:
                    if '_links' in record and record['_links']:
                        item_identifier = record["identifier"]
                        self.db.write_header(item_identifier, self.repository_id)
                        item_count = item_count + 1
                        if (item_count % self.update_log_after_numitems == 0):
                            tdelta = time.time() - self.tstart + 0.1
                            self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                                           self.formatter.humanize(
                                                                                                               tdelta),
                                                                                                           item_count / tdelta))
                if 'next' in response['_links']:
                    url = self.url.replace("/api/v2", "") + response['_links']['next']['href']
                    r = requests.request("GET", url, headers=self.headers, params=querystring)
                    response = r.json()
                    records = response['_embedded']['stash:datasets']
                else:
                    break

            self.logger.info("Found {} items in feed".format(item_count))
            return True

        except Exception as e:
            self.logger.error("Updating Dryad Repository failed: {}".format(e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True
        return False

    def format_dryad_to_oai(self, dryad_record):
        record = {}
        record["identifier"] = dryad_record["identifier"]
        record["item_url"] = "https://doi.org/" + dryad_record["identifier"].split("doi:")[1]

        is_canadian = False
        for author in dryad_record['authors']:
            if 'affiliationROR' in author and author['affiliationROR']:
                try:
                    ror_record = self.ror_data[author["affiliationROR"]]
                    try:
                        if ror_record["country"]["country_code"] == "CA":
                            is_canadian = True
                            break
                    except KeyError:
                        self.logger.error("ROR record {} missing country".format(author['affiliationROR']))
                        continue
                except KeyError:
                        self.logger.info("ROR ID {} does not exist".format(author["affiliationROR"]))
                        continue
            if not is_canadian:
                return False

        record["creator"] = []
        record["affiliation"] = []
        for creator in dryad_record["authors"]:
            creatorName = ""
            if "lastName" in creator and creator["lastName"]:
                creatorName = creator["lastName"]
            if "firstName" in creator and creator["firstName"]:
                creatorName = creatorName + ", " + creator["firstName"]
            if creatorName:
                record["creator"].append(creatorName)

            affiliation = ""
            if "affiliation" in creator and creator["affiliation"]:
                affiliation = creator["affiliation"]
            if "affiliationROR" in creator and creator["affiliationROR"]:
                affiliation = {"affiliation_name": affiliation, "affiliation_ror": creator["affiliationROR"]}
            if affiliation not in record["affiliation"]:
                record["affiliation"].append(affiliation)

        if len(record["affiliation"]) == 0:
            record.pop("affiliation")

        record["title"] = dryad_record["title"]
        record["title_fr"] = ""
        record["series"] = ""
        try:
            record["pub_date"] = dryad_record["publicationDate"]
        except Exception as e:
            record["pub_date"] = dryad_record["lastModificationDate"]
        record["description"] = dryad_record.get("abstract", "")
        record["rights"] = dryad_record["license"]

        if "keywords" in dryad_record and dryad_record["keywords"]:
            record["tags"] = dryad_record["keywords"]

        if "locations" in dryad_record and dryad_record["locations"]:
            record["geoplaces"] = []
            for location in dryad_record["locations"]:
                record["geoplaces"].append({"place_name": location["place"]})

        return record

    def _update_record(self, record):
        try:
            record_url = self.url + "/datasets/" + urllib.parse.quote_plus(record["local_identifier"])
            try:
                item_response = requests.get(record_url)
                dryad_record = json.loads(item_response.text)
            except Exception as e:
                # Exception means this URL was not found
                self.logger.error("Fetching record {} failed: {}".format(record_url, e))
                return True
            oai_record = self.format_dryad_to_oai(dryad_record)
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
                    print(dryad_record)
                except Exception as e:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def update_stale_records(self, dbparams):
        if not self.load_ror_data():
            self.logger.error("ROR data could not be fetched from remote")
            return

        if not self.ror_data:
            self.logger.error("ROR data could not be loaded from the local JSON file")
            return

        super().update_stale_records(dbparams)

