from harvester.HarvestRepository import HarvestRepository
import requests
import re


class MarkLogicRepository(HarvestRepository):
    """ MarkLogic Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "marklogic"
        super(MarkLogicRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.records_per_request = 50
        self.params = {
            "format": "json",
            "start": 0,
            "pageLength": self.records_per_request
        }
        self.query = "((*))"
        if "collection" in repoParams:
            coll = re.sub("[^a-zA-Z0-9_-]+", "", repoParams["collection"])  # Remove potentially bad chars
            self.query += "%2520AND%2520(coll:" + coll + ")"
        if "options" in repoParams:
            options = re.sub("[^a-zA-Z0-9_-]+", "", repoParams["options"])  # Remove potentially bad chars
            self.params["options"] = options

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id,
            "repo_url": self.url,
            "repo_set": self.set,
            "repo_name": self.name,
            "repo_name_fr": self.name_fr,
            "repo_type": "marklogic",
            "enabled": self.enabled,
            "repo_thumbnail": self.thumbnail,
            "item_url_pattern": self.item_url_pattern,
            "abort_after_numerrors": self.abort_after_numerrors,
            "max_records_updated_per_run": self.max_records_updated_per_run,
            "update_log_after_numitems": self.update_log_after_numitems,
            "record_refresh_days": self.record_refresh_days,
            "repo_refresh_days": self.repo_refresh_days,
            "homepage_url": self.homepage_url,
            "repo_oai_name": self.repo_oai_name,
            "repo_registry_uri": self.repo_registry_uri
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            offset = 0
            while True:
                self.params["start"] = offset
                paramstring = "requestURL=" + self.query + "%26" + "%26".join(
                    "{}%3D{}".format(k, v) for (k, v) in self.params.items())
                response = requests.get(self.url, params=paramstring,
                                        verify=False)  # Needs to be string not dict to force specific urlencoding
                records = response.json()
                if not records["results"]:
                    break
                for record in records["results"]:
                    oai_record = self.format_marklogic_to_oai(record)
                    if oai_record:
                        self.db.write_record(oai_record, self)
                offset += self.records_per_request

            return True

        except Exception as e:
            self.logger.error("Updating MarkLogic Repository failed: {} {}".format(type(e).__name__, e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False

    def format_marklogic_to_oai(self, marklogic_record):
        record = {}
        record["creator"] = []
        record["affiliation"] = []

        for entry in marklogic_record["metadata"]:
            if "AuthEnty" in entry and entry["AuthEnty"].strip() != "":
                record["creator"].append(entry["AuthEnty"].strip())
            if "AuthEnty_affiliation" in entry and entry["AuthEnty_affiliation"].strip() != "":
                record["affiliation"].append(entry["AuthEnty_affiliation"].strip())
            if "abstract" in entry and entry["abstract"].strip() != "":
                record["description"] = entry["abstract"].strip()
            if "TI-facet" in entry and entry["TI-facet"].strip() != "":
                record["title"] = entry["TI-facet"].strip()
            if "date" in entry and str(entry["date"]).strip() != "":
                record["pub_date"] = str(entry["date"]).strip()
        record["identifier"] = marklogic_record["uri"].rsplit('/', 1)[1]
        record["publisher"] = self.publisher
        record["series"] = ""
        record["title_fr"] = ""
        record["creator"] = list(set(record["creator"]))
        record["affiliation"] = list(set(record["affiliation"]))
        return record

    def _update_record(self, record):
        # There is no update for individual records, they are updated on full crawl
        return True
