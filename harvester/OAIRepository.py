import requests as requests
from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
from sickle import Sickle
from sickle.iterator import BaseOAIIterator
from sickle.models import OAIItem, Header
from sickle.oaiexceptions import IdDoesNotExist
from collections import defaultdict
import re
import dateparser
import time
import json


# import dateparser
# from time import strftime

class FRDRRecord(OAIItem):
    """ Override Sickle OAIItem to handle stripping only known namespaces """

    def __init__(self, record_element, strip_ns=False):
        super(FRDRRecord, self).__init__(record_element, strip_ns=strip_ns)
        self.header = Header(self.xml.find('.//' + self._oai_namespace + 'header'))
        self.deleted = self.header.deleted
        if not self.deleted:
            self.metadata = self.xml_to_dict(self.xml.find('.//' + self._oai_namespace + 'metadata').getchildren()[0])

    def xml_to_dict(self, tree, paths=None):
        """ Modified from Sickle.utils to strip only some namespaces """
        namespaces_to_strip = [
            'http://purl.org/dc/elements/1.1/',
            'https://schema.datacite.org/meta/kernel-3/',
            'http://www.openarchives.org/OAI/2.0/'
        ]
        paths = paths or ['.//']
        fields = defaultdict(list)
        for path in paths:
            elements = tree.findall(path, {})
            for element in elements:
                tag_namespace = re.search('\{(.*)\}', element.tag).group(1)
                if tag_namespace in namespaces_to_strip:
                    tag = re.sub(r'\{.*\}', '', element.tag)
                else:
                    tag = tag_namespace + "#" + re.sub(r'\{.*\}', '', element.tag)
                fields[tag].append(element.text)
        return dict(fields)


class FRDRItemIterator(BaseOAIIterator):
    """ Modifed from Sickle.interator.OAIItemIterator to implement custom item mapping """

    def __init__(self, sickle, params, ignore_deleted=False):
        VERBS_ELEMENTS = {
            'GetRecord': 'record',
            'ListRecords': 'record',
            'ListIdentifiers': 'header',
            'ListSets': 'set',
            'ListMetadataFormats': 'metadataFormat',
            'Identify': 'Identify',
        }
        self.mapper = FRDRRecord
        self.element = VERBS_ELEMENTS[params.get('verb')]
        super(FRDRItemIterator, self).__init__(sickle, params, ignore_deleted)

    def _next_response(self):
        super(FRDRItemIterator, self)._next_response()
        self._items = self.oai_response.xml.iterfind('.//' + self.sickle.oai_namespace + self.element)

    def next(self):
        """Return the next record/header/set."""
        while True:
            for item in self._items:
                mapped = self.mapper(item)
                if self.ignore_deleted and mapped.deleted:
                    continue
                return mapped
            if self.resumption_token and self.resumption_token.token:
                self._next_response()
            else:
                raise StopIteration


def get_frdr_filenames(base_url):
    if base_url == "":
        return ""
    full_url = base_url + "file_sizes_perm.json?download=0"
    session = requests.Session()
    session.headers.update({'referer': full_url})
    file = session.get(full_url)
    file_text = file.text
    try:
        data = json.loads(file_text)
        files = data["contents"]
        file_names = []
        for f in files:
            file_names.append(f["name"])
        return file_names
    except Exception as e:
        return ""


def get_frdr_files_size(base_url):
    if base_url == "":
        return 0
    full_url = base_url + "file_sizes_perm.json?download=0"
    session = requests.Session()
    session.headers.update({'referer': full_url})
    file = session.get(full_url)
    file_text = file.text
    try:
        data = json.loads(file_text)
        return data["size"]
    except Exception as e:
        return 0


class OAIRepository(HarvestRepository):
    """ OAI Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "oai_dc"
        self.oai_identifier_field = "identifier"
        self.default_language = "en"
        super(OAIRepository, self).setRepoParams(repoParams)
        self.sickle = Sickle(self.url, iterator=FRDRItemIterator)

    def _crawl(self):
        records = []

        try:
            if self.set is None or self.set == "":
                records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True)
            else:
                records = self.sickle.ListRecords(metadataPrefix=self.metadataprefix, ignore_deleted=True, set=self.set)
        except Exception as e:
            self.logger.info("No items were found")

        kwargs = {
            "repo_id": self.repository_id,
            "repo_url": self.url,
            "repo_set": self.set,
            "repo_name": self.name,
            "repo_name_fr": self.name_fr,
            "repo_type": "oai",
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
        item_count = 0

        while records:
            try:
                record = records.next()
                metadata = record.metadata

                # EPrints workaround for using header datestamp in lieu of date
                if "date" not in metadata and record.header.datestamp:
                    metadata["date"] = record.header.datestamp

                # Use the header id for the database key (needed later for OAI GetRecord calls)
                metadata["local_identifier"] = record.header.identifier
                if "oai:https://" in metadata["local_identifier"]:
                    metadata["local_identifier"] = metadata["local_identifier"].replace("oai:https://", "oai:")

                # Search for a hyperlink in the list of identifiers; use the first hyperlink but prefer DOIs
                metadata["item_url"] = None
                if self.item_url_pattern is not None:
                        metadata["item_url_pattern"] = self.item_url_pattern
                        metadata["item_url"] = self.db.construct_local_url(metadata)
                elif self.oai_identifier_field in metadata:
                    searchlist = metadata[self.oai_identifier_field]
                    if not isinstance(searchlist, list):
                        searchlist = [searchlist]
                    for idt in searchlist:
                        if metadata["item_url"] is None or "doi" in idt:
                            if idt.lower().startswith("http"):
                                metadata["item_url"] = idt
                            if idt.lower().startswith("doi:"):
                                metadata["item_url"] = "https://doi.org/" + idt[4:]
                            if idt.lower().startswith("hdl:"):
                                metadata["item_url"] = "https://hdl.handle.net/" + idt[4:]

                metadata["identifier"] = metadata["local_identifier"]

                oai_record = self.unpack_oai_metadata(metadata, record.xml)
                self.domain_metadata = self.find_domain_metadata(metadata)
                print("Writing local_id=" + metadata["local_identifier"] + " id=" + metadata["identifier"] + " url=" + metadata["item_url"])
                self.db.write_record(oai_record, self)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info(
                        "Done {} items after {} ({:.1f} items/sec)".format(item_count, self.formatter.humanize(tdelta),
                                                                           (item_count / tdelta)))

            except AttributeError:
                self.logger.debug("AttributeError while working on item {}".format(item_count))

            except StopIteration:
                break

            except Exception as e:
                self.logger.debug("Exception while working on item {}: {} {}".format(item_count, type(e).__name__, e))

        self.logger.info("Processed {} items in feed".format(item_count))

    def unpack_oai_metadata(self, record, record_xml):
        record["pub_date"] = record.get("date")

        if self.metadataprefix.lower() == "ddi":
            # Mapping as per http://www.ddialliance.org/resources/ddi-profiles/dc
            record["title"] = record.get("titl")
            record["creator"] = record.get("AuthEnty")
            record["tags"] = record.get("keyword", [])
            if "topcClas" in record and len(record["topcClas"]) > 0:
                record["tags"].extend(filter(None, record["topcClas"]))
            record["description"] = record.get("abstract")
            record["publisher"] = record.get("producer")
            record["contributor"] = record.get("othId")
            record["pub_date"] = record.get("prodDate")
            record["type"] = record.get("dataKind")
            record["identifier"] = record.get("IDNo")
            record["rights"] = record.get("copyright")

        if self.metadataprefix.lower() == "fgdc" or self.metadataprefix.lower() == "fgdc-std":
            record["creator"] = []
            for creator in record.get("origin"):
                if creator not in record["creator"]:
                    record["creator"].append(creator)
            record["tags"] = record.get("themekey")
            record["description"] = record.get("abstract")
            record["publisher"] = record.get("cntorg")
            # Put these dates in preferred order
            record["pub_date"] = [record.get("pubdate"), record.get("begdate"), record.get("enddate")]
            record["type"] = record.get("geoform")
            record["item_url"] = record.get("onlink")
            record["rights"] = record.get("distliab")
            record["access"] = record.get("accconst")

            if "placekt" in record:
                record["coverage"] = record["placekt"]

            if "bounding" in record:
                record["geobboxes"] = [{"westLon": record["westbc"][0], "eastLon": record["eastbc"][0],
                                        "northLat": record["northbc"][0], "southLat": record["southbc"][0]}]

        # Parse FRDR records
        if self.metadataprefix.lower() == "frdr":
            frdr_xml = record_xml.find("{http://www.openarchives.org/OAI/2.0/}metadata").find("{https://www.frdr-dfdr.ca/schema/1.0/}frdr")

            # Add CRDC from full XML
            record["crdc"] = []
            for crdc_xml in frdr_xml.findall("{https://www.frdr-dfdr.ca/schema/1.0/}crdc"):
                crdc_entry = {}
                crdc_entry["crdc_code"] = crdc_xml.find("{https://www.frdr-dfdr.ca/schema/1.0/}crdcCode").text
                for crdc_group in crdc_xml.findall("{https://www.frdr-dfdr.ca/schema/1.0/}crdcGroup"):
                    if crdc_group.get("{http://www.w3.org/XML/1998/namespace}lang") == "en":
                        crdc_entry["crdc_group_en"] = crdc_group.text
                    elif crdc_group.get("{http://www.w3.org/XML/1998/namespace}lang") == "fr":
                        crdc_entry["crdc_group_fr"] = crdc_group.text
                for crdc_class in crdc_xml.findall("{https://www.frdr-dfdr.ca/schema/1.0/}crdcClass"):
                    if crdc_class.get("{http://www.w3.org/XML/1998/namespace}lang") == "en":
                        crdc_entry["crdc_class_en"] = crdc_class.text
                    elif crdc_class.get("{http://www.w3.org/XML/1998/namespace}lang") == "fr":
                        crdc_entry["crdc_class_fr"] = crdc_class.text
                for crdc_field in crdc_xml.findall("{https://www.frdr-dfdr.ca/schema/1.0/}crdcField"):
                    if crdc_field.get("{http://www.w3.org/XML/1998/namespace}lang") == "en":
                        crdc_entry["crdc_field_en"] = crdc_field.text
                    elif crdc_field.get("{http://www.w3.org/XML/1998/namespace}lang") == "fr":
                        crdc_entry["crdc_field_fr"] = crdc_field.text
                record["crdc"].append(crdc_entry)
            if len(record["crdc"]) == 0:
                record.pop("crdc")

            # Add creators and affiliations from full XML
            record["creator"] = []
            record["affiliation"] = []

            if len(frdr_xml.find("{http://datacite.org/schema/kernel-4}creators")) > 0:
                for creator_xml in list(frdr_xml.find("{http://datacite.org/schema/kernel-4}creators")):
                    creator_name = creator_xml.find("{http://datacite.org/schema/kernel-4}creatorName")
                    creator_orcid = creator_xml.find("{http://datacite.org/schema/kernel-4}nameIdentifier")
                    if creator_name is not None and creator_orcid is not None:
                        record["creator"].append({"name":creator_name.text, "orcid":creator_orcid.text})
                    elif creator_name is not None:
                        record["creator"].append(creator_name.text)
                    for affiliation_xml in creator_xml.findall("{http://datacite.org/schema/kernel-4}affiliation"):
                        # This is currently per-record affiliation, not per-author
                        if affiliation_xml.get("affiliationIdentifier"):
                            record["affiliation"].append({"affiliation_name": affiliation_xml.text.strip(),
                                                          "affiliation_ror": affiliation_xml.get("affiliationIdentifier")})
                        else:
                            record["affiliation"].append(affiliation_xml.text.strip())

            if "dateissued" in record:
                record["pub_date"] = record["dateissued"]

            if "http://datacite.org/schema/kernel-4#geolocationPlace" in record:
                record["geoplaces"] = []
                for geo_place in record["http://datacite.org/schema/kernel-4#geolocationPlace"]:
                    place_split = geo_place.split(';')
                    if len(place_split) == 4 or (len(place_split) == 5 and place_split[4] == ""):
                        place = {}
                        place["country"] = place_split[3]
                        place["province_state"] = place_split[2]
                        place["city"] = place_split[1]
                        place["other"] = place_split[0]
                        # FIXME replace placeholder "-" for consistency with Dataverse
                    else:
                        place = {"place_name": geo_place}
                    if place not in record["geoplaces"]:
                        record["geoplaces"].append(place)

            if "http://datacite.org/schema/kernel-4#geolocationPoint" in record:
                record["geopoints"] = []
                for geopoint in record["http://datacite.org/schema/kernel-4#geolocationPoint"]:
                    point_split = geopoint.split()
                    if len(point_split) == 2:
                        record["geopoints"].append({"lat": point_split[0], "lon": point_split[1]})

            if "http://datacite.org/schema/kernel-4#geolocationBox" in record:
                record["geobboxes"] = []
                for geobbox in record["http://datacite.org/schema/kernel-4#geolocationBox"]:
                    boxcoordinates = geobbox.split()
                    if len(boxcoordinates) == 4:
                        record["geobboxes"].append({"southLat": boxcoordinates[0], "westLon": boxcoordinates[1],
                                                    "northLat": boxcoordinates[2], "eastLon": boxcoordinates[3]})

            # Look for datacite.creatorAffiliation
            if "http://datacite.org/schema/kernel-4#creatorAffiliation" in record:
                record["affiliation"] = record.get("http://datacite.org/schema/kernel-4#creatorAffiliation")

            # Add contributors
            record["contributor"] = []
            for contributorType in ["DataCollector", "DataManager", "ProjectManager", "ResearchGroup", "Sponsor", "Supervisor", "Other"]:
                dataciteContributorType = 'http://datacite.org/schema/kernel-4#contributor' + contributorType
                if dataciteContributorType in record:
                    for contributor in record[dataciteContributorType]:
                        if contributor not in record["contributor"]:
                            record["contributor"].append(contributor)

            if len(record["contributor"]) == 0:
                record.pop("contributor")
            #File info base

            endpoint_hostname = record.get("https://www.frdr-dfdr.ca/schema/1.0/#globusHttpsHostname", [""])[0]
            if endpoint_hostname and not endpoint_hostname.startswith("https"):
                endpoint_hostname = "https://" + endpoint_hostname
            endpoint_path = record.get("https://www.frdr-dfdr.ca/schema/1.0/#globusEndpointPath", [""])[0]

            # Get all File sizes
            try:
                sizes = get_frdr_files_size(endpoint_hostname + endpoint_path)
                if not record.get("files_size") == sizes:
                    record["files_altered"] = 1
                    record["files_size"] = sizes
                    record["geodisy_harvested"] = 0
                if sizes > self.geo_files_limit_bytes:
                    self.logger.info("File sizes " + str(sizes) + " over download limit for record: " + record["local_identifier"])
                    record["files_altered"] = 0
                    record["geodisy_harvested"] = 1
            except Exception as e:
                self.logger.error(
                    "Something wrong trying to access files length from hostname: {} , path: {}".format(
                        endpoint_hostname,
                        endpoint_path))

            # Get geospatial files
            if "geodisy_harvested" not in record or record["geodisy_harvested"] == 0:
                try:
                    filenames = get_frdr_filenames(endpoint_hostname + endpoint_path)
                    # Get File Download URLs
                    for f in filenames:
                        file_segments = len(f.split("."))
                        extension = "." + f.split(".")[file_segments - 1]
                        if extension.lower() in self.geofile_extensions:
                            geofile = {}
                            geofile["filename"] = f
                            geofile["uri"] = endpoint_hostname + endpoint_path + "submitted_data/" + f
                            record.setdefault("geofiles", []).append(geofile)
                except Exception as e:
                    self.logger.error("Something wrong trying to access files from hostname: {} , path: {}".format(endpoint_hostname, endpoint_path))

        if record["pub_date"] is None:
            return None

        if self.oai_identifier_field not in record:
            return None

        if "creator" in record and record["creator"]:
            if isinstance(record["creator"], list):
                # Only keep creators that aren't Nones
                record["creator"] = [x for x in record["creator"] if x != None]
        else:
            record["creator"] = []
            if self.metadataprefix.lower() == "fgdc-std":
                # Workaround for WOUDC, which doesn't attribute individual datasets
                record["creator"].append(self.name)
            elif "contributor" in record:
                if isinstance(record["contributor"], list):
                    record["creator"] = [x for x in record["contributor"] if x != None]
                else:
                    record["creator"].append(record["contributor"])
                record.pop("contributor") # don't duplicate contributors and creators
            elif "publisher" in record:
                if isinstance(record["publisher"], list):
                    record["creator"] = [x for x in record["publisher"] if x != None]
                else:
                    record["creator"].append(record["publisher"])

        if "creator" in record and isinstance(record["creator"], list) and len(record["creator"]) == 0:
            self.logger.debug("Item {} is missing creator - will not be added".format(record["local_identifier"]))
            return None

        if "contributor" in record and record["contributor"]:
            # Only keep contributors that aren't Nones
            if isinstance(record["contributor"], list):
                record["contributor"] = [x for x in record["contributor"] if x != None]
                if len(record["contributor"]) == 0:
                    record.pop("contributor")

        # If date is undefined add an empty key
        if "pub_date" not in record:
            record["pub_date"] = ""

        # If there are multiple dates choose the longest one (likely the most specific)
        # If there are a few dates with the same length the first one will be used, which assumes we grabbed them in a preferred order
        # Exception test added for some strange PDC dates of [null, null]
        if isinstance(record["pub_date"], list):
            valid_date = record["pub_date"][0] or ""
            for datestring in record["pub_date"]:
                if datestring is not None:
                    if len(datestring) > len(valid_date):
                        valid_date = datestring
            record["pub_date"] = valid_date

        # If date is still a one-value list, make it a string
        if isinstance(record["pub_date"], list):
            record["pub_date"] = record["pub_date"][0]
        # If a date has question marks, chuck it
        if "?" in record["pub_date"]:
            return None

        try:
            date_object = dateparser.parse(record["pub_date"])
            if date_object is None:
                date_object = dateparser.parse(record["pub_date"], date_formats=["%Y%m%d"])
            record["pub_date"] = date_object.strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.error("Something went wrong parsing the date, {} from {}".format(record["pub_date"], record["local_identifier"]))
            return None

        if "title" not in record:
            return None

        language = self.default_language
        if "language" in record:
            if isinstance(record["language"], list):
                if record["language"][0]: # take the first language if it isn't None
                    record["language"] = record["language"][0].strip()
                    record["language"] = record["language"].lower()
                else:
                    record["language"] = ""
            if record["language"] in ["fr", "fre", "fra", "french"]:
                language = "fr"

        if language == "fr":
            if isinstance(record["title"], list):
                record["title_fr"] = record["title"][0].strip()
            else:
                record["title_fr"] = record["title"].strip()
            # Remove "title" from record since this is the English field
            record["title"] = ""

            if "tags_fr" not in record:
                record["tags_fr"] = record.get("subject")
                record.pop("subject", None)
                if record["tags_fr"] is None:
                    record.pop("tags_fr")
        else:
            if isinstance(record["title"], list):
                record["title"] = record["title"][0].strip()
            else:
                record["title"] = record["title"].strip()
            record["title_fr"] = ""

            if "tags" not in record:
                record["tags"] = record.get("subject")
                record.pop("subject", None)
                if record["tags"] is None:
                    record.pop("tags")

        if "publisher" in record:
            if isinstance(record["publisher"], list):
                record["publisher"] = record["publisher"][0]

        if "series" not in record:
            record["series"] = ""

        if "coverage" in record and not record["coverage"] == [None]:
            record["geoplaces"] = []
            if self.name == "SFU Radar":
                record["coverage"] = [x.strip() for x in record["coverage"][0].split(";")]
            if not isinstance(record["coverage"], list):
                record["coverage"] = [record["coverage"]]
            for place_name in record["coverage"]:
                if place_name and place_name.lower().islower(): # to filter out dates, confirm at least one letter
                    record["geoplaces"].append({"place_name": place_name})

        # DSpace workaround to exclude theses and non-data content
        if self.prune_non_dataset_items:
            if record["type"] and "Dataset" not in record["type"]:
                return None

        if "rights" in record and isinstance(record["rights"], list):
            record["rights"] = list(set(filter(None.__ne__, record["rights"]))) # Remove duplicates and Nones from Rights (FRDR, Eprints)
            record["rights"].sort() # Ensure consistent order
            record["rights"] = "\n".join(record["rights"]) # Join all rights entries into one

        return record

    def find_domain_metadata(self, record):
        # Exclude fundingReference and nameIdentifier; need a way to group linked fields in display first
        excludedElements = ["http://datacite.org/schema/kernel-4#resourcetype",
                    "http://datacite.org/schema/kernel-4#creatorAffiliation",
                    "http://datacite.org/schema/kernel-4#publicationyear",
                    "http://datacite.org/schema/kernel-4#geolocationPlace",
                    "http://datacite.org/schema/kernel-4#geolocationPoint",
                    "http://datacite.org/schema/kernel-4#geolocationBox",
                    "https://www.frdr-dfdr.ca/schema/1.0/#globusEndpointName",
                    "https://www.frdr-dfdr.ca/schema/1.0/#globusEndpointPath",
                    "https://www.frdr-dfdr.ca/schema/1.0/#globusHttpsHostname",
                    "http://datacite.org/schema/kernel-4#contributorDataCollector",
                    "http://datacite.org/schema/kernel-4#contributorDataManager",
                    "http://datacite.org/schema/kernel-4#contributorProjectManager",
                    "http://datacite.org/schema/kernel-4#contributorResearchGroup",
                    "http://datacite.org/schema/kernel-4#contributorSponsor",
                    "http://datacite.org/schema/kernel-4#contributorSupervisor",
                    "http://datacite.org/schema/kernel-4#contributorOther",
                    "http://datacite.org/schema/kernel-4#creatorNameIdentifier",
                    "http://datacite.org/schema/kernel-4#fundingReferenceFunderName",
                    "http://datacite.org/schema/kernel-4#fundingReferenceAwardNumber",
                    "http://datacite.org/schema/kernel-4#fundingReferenceAwardTitle",
                    "https://www.frdr-dfdr.ca/schema/1.0/#crdc",
                    "https://www.frdr-dfdr.ca/schema/1.0/#crdcCode",
                    "https://www.frdr-dfdr.ca/schema/1.0/#crdcGroup",
                    "https://www.frdr-dfdr.ca/schema/1.0/#crdcClass",
                    "'https://www.frdr-dfdr.ca/schema/1.0/#crdcField'"]
        newRecord = {}
        for elementName in list(record):
            if '#' in elementName:
                if not [ele for ele in excludedElements if(ele in elementName)]:
                    newRecord[elementName] = record.pop(elementName, None)
        return newRecord

    @rate_limited(5)
    def _update_record(self, record):
        #self.logger.debug("Updating OAI record {} {}".format(record["local_identifier"], record["item_url"]))

        try:
            single_record = self.sickle.GetRecord(identifier=record["local_identifier"], metadataPrefix=self.metadataprefix)

            try:
                metadata = single_record.metadata
            except AttributeError:
                metadata = {}

            # EPrints workaround for using header datestamp in lieu of date
            if "date" not in metadata and single_record.header.datestamp:
                metadata["date"] = single_record.header.datestamp

            metadata["local_identifier"] = record["local_identifier"] # OAI identifier format "oai:repo:record"
            metadata["identifier"] = record["local_identifier"]
            metadata["item_url"] = record["item_url"]
            metadata["geodisy_harvested"] = 0
            if "geodisy_harvested" in record:
                metadata["geodisy_harvested"] = record["geodisy_harvested"]
            metadata["geodisy_harvested"] = 0
            if "fizes_size" in record:
                metadata["fizes_size"] = record["fizes_size"]
            oai_record = self.unpack_oai_metadata(metadata, single_record.xml)
            self.domain_metadata = self.find_domain_metadata(metadata)
            if oai_record is None:
                self.db.delete_record(record)
                return False
            self.db.write_record(oai_record, self)
            return True

        except IdDoesNotExist:
            # Item no longer in this repo
            self.db.delete_record(record)
            return True

        except Exception as e:
            self.logger.error("Updating item failed (repo_id:{}, oai_id:{}): {} {}".format(
                self.repository_id, record["local_identifier"], type(e).__name__, e))
            if self.dump_on_failure == True:
                try:
                    print(single_record.metadata)
                except Exception as e:
                    pass
            # Touch the record so we do not keep requesting it on every run
            self.db.touch_record(record)
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True

        return False
