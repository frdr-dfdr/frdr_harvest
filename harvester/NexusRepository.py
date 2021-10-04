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
        self.headers = {'accept': 'application/vnd.api+json'}

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
            "repo_oai_name": self.repo_oai_name
        }
        self.repository_id = self.db.update_repo(**kwargs)

        try:
            item_count = 0
            query_url = self.url
            while query_url:
                querystring = {"type": "https://schema.org/Dataset"}
                response = requests.request("GET", query_url, headers=self.headers, params=querystring)
                response = response.json()
                if "_next" in response:
                    query_url = response["next"]
                else:
                    break
                for record in response["_results"]:
                    item_identifier = record["_self"]
                    self.db.write_header(item_identifier, self.repository_id)
                    item_count = item_count + 1
                    if (item_count % self.update_log_after_numitems == 0):
                        tdelta = time.time() - self.tstart + 0.1
                        self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(item_count,
                                                                                                   self.formatter.humanize(
                                                                                                       tdelta),
                                                                                                   item_count / tdelta))
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

        record = {}

        record["series"] = ""
        record["title_fr"] = ""

        # FIXME update crosswalk for DATS metadata
        if ("name" in nexus_record) and nexus_record["name"]:
            record["title"] = nexus_record["name"]

        if ("description" in nexus_record) and nexus_record["description"]:
            record["description"] = nexus_record["description"]

        if ("creator" in nexus_record) and nexus_record["creator"]:
            if ("name" in nexus_record["creator"]) and nexus_record["creator"]["name"]:
                    record["creator"] = nexus_record["creator"]["name"]

        if ("keywords" in nexus_record) and nexus_record["keywords"]:
            if isinstance(nexus_record["keywords"], str):
                record["tags"] = nexus_record["keywords"].split(",")

        if ("publisher" in nexus_record) and nexus_record["publisher"]:
            if ("name" in nexus_record["publisher"]) and nexus_record["publisher"]["name"]:
                record["publisher"] = nexus_record["publisher"]["name"]

        if ("datePublished" in nexus_record) and nexus_record["datePublished"]:
            record["pub_date"] = parser.parse(nexus_record["datePublished"]).strftime('%Y-%m-%d')

        if ("identifier" in nexus_record) and nexus_record["identifier"]:
            if ("url" in nexus_record["identifier"]) and nexus_record["identifier"]["url"]:
                record["item_url"] = nexus_record['identifier']["url"]

        if "isAccessibleForFree" in nexus_record:
            if nexus_record["isAccessibleForFree"]:
                record["access"] = "Public"
            else:
                record["access"] = "Limited"

        if ("license" in nexus_record) and nexus_record["license"]:
            record["rights"] = nexus_record["license"]

        if ("url" in nexus_record) and nexus_record["url"]:
            record["identifier"] = nexus_record["url"]

        if ("spatialCoverage" in nexus_record) and nexus_record["spatialCoverage"]:
            try:
                boxcoordinates = nexus_record["spatialCoverage"]["geo"]["box"].split()
                if len(boxcoordinates) == 4:
                    record["geobboxes"] = [{"westLon": boxcoordinates[0], "southLat": boxcoordinates[1],
                                            "eastLon": boxcoordinates[2], "northLat": boxcoordinates[3]}]
            except Exception as e:
                pass

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
            item_response_content = item_response.read().decode('utf-8')
            item_json = json.loads(item_response_content)

            oai_record = self.format_nexus_to_oai(item_json)
            if oai_record:
                self.db.write_record(oai_record, self)
            return True
        except Exception as e:
            self.logger.error("Updating record {} failed: {} {}".format(record['local_identifier'], type(e).__name__, e))
            if self.dump_on_failure == True:
                try:
                    print(item_response_content)
                except Exception as e:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False
    