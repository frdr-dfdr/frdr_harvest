from harvester.HarvestRepository import HarvestRepository
from harvester.rate_limited import rate_limited
import time
import requests
import lxml.etree as ET
from rdflib import Graph, DCAT
from dateutil import parser
import urllib
import re

class CSWRepository(HarvestRepository):
    """ CSW Repository """

    def setRepoParams(self, repoParams):
        self.metadataprefix = "csw"
        super(CSWRepository, self).setRepoParams(repoParams)
        self.domain_metadata = []
        self.headers = {'accept': 'application/rdf+xml'}

    def _crawl(self):
        kwargs = {
            "repo_id": self.repository_id, "repo_url": self.url, "repo_set": self.set, "repo_name": self.name,
            "repo_type": "csw",
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
            g = Graph()
            request_success = False
            request_count = 0
            while not request_success and request_count < 5:
                try:
                    g.parse(self.url)
                    request_success = True
                except urllib.error.HTTPError as e:
                    request_count +=1
                    self.logger.info("Trying again to fetch records at {}: {}".format(self.url, e))
                    time.sleep(1)

            item_count = 0
            for s, p, o in g.triples((None, None, DCAT.CatalogRecord)):
                # Get record identifier
                identifier = s.split("https://hecate.hakai.org/geonetwork/srv/metadata//records/")[-1]
                self.db.write_header(identifier, self.item_url_pattern, self.repository_id)
                item_count = item_count + 1
                if (item_count % self.update_log_after_numitems == 0):
                    tdelta = time.time() - self.tstart + 0.1
                    self.logger.info("Done {} item headers after {} ({:.1f} items/sec)".format(
                        item_count, self.formatter.humanize(tdelta), item_count / tdelta))
            self.logger.info("Found {} items in feed".format(item_count))
            return True

        except Exception as e:
            self.logger.error("Updating CSW Repository failed: {} {}".format(type(e).__name__, e))
            self.error_count = self.error_count + 1
            if self.error_count < self.abort_after_numerrors:
                return True
        return False

    def format_csw_to_oai(self, csw_record, local_identifier):
        # Find one child element using a namespace
        def find_ns(parent, tag):
            if parent is not None:
                if len(parent.findall(tag, csw_record.nsmap)) > 1:
                    self.logger.info("find_ns() called on element with more than one child: {}, {}".format(parent.tag, tag))
                return parent.find(tag, csw_record.nsmap)
            else:
                return None

        # Find all child element using a namespace
        def findall_ns(parent, tag):
            if parent is not None:
                return parent.findall(tag, csw_record.nsmap)
            else:
                return None

        # Get the gco:CharacterString for a given element
        def get_gco_CharacterString(element):
            if find_ns(element, "gco:CharacterString") is not None:
                return find_ns(element, "gco:CharacterString").text
            return None

        record = {}

        # todo Examples:
        # "https://hecate.hakai.org/geonetwork/srv/api/records/23bc8c35-2e4e-4382-9296-a52d5ea49889/formatters/xml"
        # "https://hecate.hakai.org/geonetwork/srv/api/records/e2d3d616-9ee2-451f-8584-14801b4c6fd0/formatters/xml"
        # todo Documentation:
        # https://docs.meridian.cs.dal.ca/metadata/Metadata.html is useful as a rough guide to ISO 19115, although they made changes

        # Shortcuts to frequently used nodes:
        data_identification = find_ns(find_ns(csw_record, "gmd:identificationInfo"), "gmd:MD_DataIdentification")
        citation = find_ns(find_ns(data_identification, "gmd:citation"), "gmd:CI_Citation")

        # Title and description
        record["identifier"] = local_identifier
        record["series"] = ""
        record["title"] = get_gco_CharacterString(find_ns(citation, "gmd:title")).strip()
        record["title_fr"] = ""
        record["description"] = get_gco_CharacterString(find_ns(data_identification, "gmd:abstract"))

        # Item URL: use DOI if available
        record["item_url"] = self.item_url_pattern.replace("%id%", local_identifier)
        if find_ns(csw_record, "gmd:dataSetURI") is not None:
            datasetURIString = get_gco_CharacterString(find_ns(csw_record, "gmd:dataSetURI"))
            if datasetURIString is not None:
                if datasetURIString.startswith("ttp"):  # fix typo in metadata
                    datasetURIString = "h" + datasetURIString
                if datasetURIString.startswith("http"):
                    record["item_url"] = datasetURIString
                    is_confirmed_doi = False
                    for doi_start in ["https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/"]:
                        if datasetURIString.startswith(doi_start):
                            is_confirmed_doi = True
                            record["item_url"] = record["item_url"].replace("http://", "https://")
                            record["item_url"] = record["item_url"].replace("https://dx.doi.org/", "https://doi.org/")
                            break
                    if not is_confirmed_doi:
                        record["item_url"] = self.item_url_pattern.replace("%id%", local_identifier)
                elif re.search("10.\d{4,9}\/[-._;()\/:a-zA-Z0-9]+$", datasetURIString):
                    record["item_url"] = "https://doi.org/{}".format(re.search("10.\d{4,9}\/[-._;()\/:a-zA-Z0-9]+$", datasetURIString)[0])

        # Creators, affiliations
        ci_responsible_parties = []
        record["creator"] = []
        record["affiliation"] = []

        # Get responsible parties from gmd:contact
        for contact in findall_ns(csw_record, "gmd:contact"):
            ci_responsible_parties.extend(findall_ns(contact, "gmd:CI_ResponsibleParty"))

        # Get responsible parties from gmd:identificationInfo / gmd:MD_DataIdentification / gmd:citation / gmd:CI_Citation / gmd:citedResponsibleParty
        for citedResponsibleParty in findall_ns(citation, "gmd:citedResponsibleParty"):
            ci_responsible_parties.append(find_ns(citedResponsibleParty, "gmd:CI_ResponsibleParty"))

        # Get responsible parties from gmd:identificationInfo / gmd:MD_DataIdentification / gmd:pointOfContact
        for pointOfContact in findall_ns(data_identification, "gmd:pointOfContact"):
            ci_responsible_parties.append(find_ns(pointOfContact, "gmd:CI_ResponsibleParty"))

        # Extract creators and affiliations from responsible parties
        for ci_responsible_party in ci_responsible_parties:
            if find_ns(ci_responsible_party, "gmd:individualName") is not None:  # Individual creator
                record["creator"].append(get_gco_CharacterString(find_ns(ci_responsible_party, "gmd:individualName")))
                if find_ns(ci_responsible_party, "gmd:organisationName") is not None:  # Individual creator has affiliation
                    record["affiliation"].append(get_gco_CharacterString(find_ns(ci_responsible_party, "gmd:organisationName")))
            elif find_ns(ci_responsible_party, "gmd:organisationName") is not None:  # Organizational creator
                record["creator"].append(get_gco_CharacterString(find_ns(ci_responsible_party, "gmd:organisationName")))

        # Remove duplicates from creators and affiliations
        record["creator"] = list(set(record["creator"]))
        record["creator"] = [x for x in record["creator"] if x is not None and "@" not in x]
        record["affiliation"] = list(set(record["affiliation"]))

        # Publication date
        citation_dates = {}
        for date in findall_ns(citation, "gmd:date"):
            ci_date = find_ns(date, "gmd:CI_Date")
            try:
                date_value = ""
                if find_ns(find_ns(ci_date, "gmd:date"), "gco:Date") is not None:
                    date_value = parser.parse(find_ns(find_ns(ci_date, "gmd:date"), "gco:Date").text).strftime('%Y-%m-%d')
                elif find_ns(find_ns(ci_date, "gmd:date"), "gco:DateTime") is not None:
                    date_value = parser.parse(find_ns(find_ns(ci_date, "gmd:date"), "gco:DateTime").text).strftime('%Y-%m-%d')
                date_type = find_ns(find_ns(ci_date, "gmd:dateType"), "gmd:CI_DateTypeCode").attrib["codeListValue"]
                citation_dates[date_type] = date_value
            except AttributeError as e:
                self.logger.error("Record {} encountered error parsing CI_Date: {}".format(local_identifier, e))

        if "publication" in citation_dates.keys():
            record["pub_date"] = citation_dates["publication"]
        elif "revision" in citation_dates.keys():
            record["pub_date"] = citation_dates["revision"]
        elif "creation" in citation_dates.keys():
            record["pub_date"] = citation_dates["creation"]
        else:
            self.logger.error("Record {} missing publication, revision, and creation dates")  # TODO investigate if this happens

        # Tags
        record["tags"] = []
        for descriptive_keywords in findall_ns(data_identification, "gmd:descriptiveKeywords"):
            for keyword in findall_ns(find_ns(descriptive_keywords, "gmd:MD_Keywords"), "gmd:keyword"):
                keyword_text = get_gco_CharacterString(keyword)
                if keyword_text is not None:
                    record["tags"].append(keyword_text.strip())

        # Subjects
        record["subject"] = []
        for topic_category in findall_ns(data_identification, "gmd:topicCategory"):
            topic_category_code = find_ns(topic_category, "gmd:MD_TopicCategoryCode")
            if topic_category_code is not None:
                record["subject"].append(topic_category_code.text.strip())

        # Rights and access
        record["rights"] = []
        record["access"] = "Public"

        access_codes = ["restricted", "otherRestrictions", "unrestricted", "private", "statutory", "confidential", "SBU", "in-confidence"]
        rights_codes = ["copyright", "patent", "patentPending", "trademark", "license", "intellectualPropertyRights", "licenseUnrestricted", "licenceEndUser", "licenceDistributor"]

        for resource_constraint in findall_ns(data_identification, "gmd:resourceConstraints"):
            md_constraints = find_ns(resource_constraint, "gmd:MD_Constraints")
            if md_constraints is not None:
                useLimitation = find_ns(md_constraints, "gmd:useLimitation")
                if useLimitation is not None and get_gco_CharacterString(useLimitation) is not None:
                    record["rights"].append(get_gco_CharacterString(useLimitation))

            md_legal_constraints = find_ns(resource_constraint, "gmd:MD_LegalConstraints")
            if md_legal_constraints is not None:
                useLimitation = find_ns(md_legal_constraints, "gmd:useLimitation")
                if useLimitation is not None and get_gco_CharacterString(useLimitation) is not None:
                    record["rights"].append(get_gco_CharacterString(useLimitation))

                # Get md_restriction_codes from access and use constraints
                md_restriction_codes = []
                access_constraints = findall_ns(md_legal_constraints, "gmd:accessConstraints")
                for access_constraint in access_constraints:
                    md_restriction_code = find_ns(access_constraint, "gmd:MD_RestrictionCode").attrib["codeListValue"]
                    md_restriction_codes.append(md_restriction_code)
                use_constraints = findall_ns(md_legal_constraints, "gmd:useConstraints")
                for use_constraint in use_constraints:
                    md_restriction_code = find_ns(use_constraint, "gmd:MD_RestrictionCode").attrib["codeListValue"]
                    md_restriction_codes.append(md_restriction_code)

                # Add md_restriction_codes to rights and access, depending onc ode
                for md_restriction_code in md_restriction_codes:
                    if md_restriction_code in access_codes and md_restriction_code != "unrestricted":
                        record["access"] = md_restriction_code
                    if md_restriction_code in rights_codes:
                        record["rights"].append(md_restriction_code)

        # If record is French, swap fields
        language = ""
        if find_ns(find_ns(csw_record, "gmd:language"), "gmd:LanguageCode") is not None:
            language = find_ns(find_ns(csw_record, "gmd:language"), "gmd:LanguageCode").attrib["codeListValue"]
        elif get_gco_CharacterString(find_ns(csw_record, "gmd:language")) is not None:
            language = get_gco_CharacterString(find_ns(csw_record, "gmd:language"))

        if language == "fre":
            record["title_fr"] = record["title"]
            record["title"] = ""
            record["tags_fr"] = record["tags"]
            record["tags"] = []
            record["subject_fr"] = record["subject"]
            record["subject"] = []
            record["description_fr"] = record["description"]
            record["description"] = ""


        # Geospatial
        extents = findall_ns(data_identification, "gmd:extent")
        if extents is not None:
            record["geobboxes"] = []
            record["geoplaces"] = []
            for extent in extents:
                ex_extent = find_ns(extent, "gmd:EX_Extent")
                geographicElements = findall_ns(ex_extent, "gmd:geographicElement")
                # Geographic extent (ignore temporal)
                if geographicElements is not None:
                    record["geoplaces"].append({"place_name": get_gco_CharacterString(find_ns(ex_extent, "gmd:description"))})
                    # Geospatial - bounding boxes only, not polygons
                    for geographicElement in geographicElements:
                        geographicBoundingBox = find_ns(geographicElement, "gmd:EX_GeographicBoundingBox")
                        record["geobboxes"].append({"westLon": find_ns(find_ns(geographicBoundingBox,"gmd:westBoundLongitude"), "gco:Decimal").text,
                                                    "eastLon": find_ns(find_ns(geographicBoundingBox, "gmd:eastBoundLongitude"),"gco:Decimal").text,
                                                    "northLat": find_ns(find_ns(geographicBoundingBox, "gmd:northBoundLatitude"), "gco:Decimal").text,
                                                    "southLat": find_ns(find_ns(geographicBoundingBox, "gmd:southBoundLatitude"), "gco:Decimal").text})

        return record

    @rate_limited(5)
    def _update_record(self, record):
        request_success = False
        request_count = 0
        while not request_success and request_count < 5:
            try:
                xml_record_url = "https://hecate.hakai.org/geonetwork/srv/api/records/{}/formatters/xml".format(record["local_identifier"])
                response = requests.request("GET", xml_record_url)
                if response.status_code == 200:
                    request_success = True
                elif response.status_code == 400:
                    request_count += 1
                    self.logger.info("Trying again to fetch record {}: Response {}".format(record["local_identifier"], response.status_code))
                    time.sleep(1)
                else:
                    break
            except Exception as e:
                self.logger.error("Unable to update record {}: {} {}".format(record['local_identifier'], type(e).__name__, e))
                self.db.delete_record(record)
                return False

        if response.status_code == 200:
            try:
                xml_record = ET.fromstring(str.encode(response.text))
            except ET.XMLSyntaxError as e:
                self.logger.error("Unable to parse record {}: {}, {}".format(record['local_identifier'], type(e).__name__, e))
                self.db.touch_record(record) # Record has XML error, try again later
                return True
            oai_record = self.format_csw_to_oai(xml_record, record["local_identifier"])

            if oai_record:
                try:
                    self.db.write_record(oai_record, self)
                except Exception as e:
                    self.logger.error(
                        "Updating record {} failed: {} {}".format(record['local_identifier'], type(e).__name__, e))
                    if self.dump_on_failure == True:
                        try:
                            print(response.text)
                        except Exception as e:
                            pass
        elif response.status_code == 404:
            # Record not found delete it
            self.db.delete_record(record)
            return True
        else: # includes 400, try again later
            self.logger.error("Unable to fetch record {} after {} tries".format(record['local_identifier'], request_count))
            self.db.touch_record(record)
            return True
        
        return True

