# FRDR Harvester

This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently supports harvesting with the following standards and APIs:

- Repositories using any of these platforms:
    - [Dataverse](https://dataverse.org/)
    - [CKAN](https://ckan.org/)
    - [Socrata](https://dev.socrata.com/)
    - [OpenDataSoft](https://www.opendatasoft.com/)
    - [ArcGIS](https://www.esri.com/en-us/arcgis/products/arcgis-open-data)
    - [MarkLogic](https://www.marklogic.com/)
- Repositories that implement either of these standards:
    - [OAI-PMH](https://www.openarchives.org/pmh/)
    - [Catalogue Service for the Web (CSW)](https://www.ogc.org/standards/cat)
- Repositories that register DOIs with [DataCite](https://datacite.org/).
- Repositories with a custom REST API, sitemap, or other means of providing machine-readable metadata, on a case-by-case basis.


## Configuration
Configuration is split into two files:

- The first controls the operation of the indexer, and is located in conf/harvester.conf.
- The list of repositories to be crawled is in conf/repos.json, in a structure like this:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
    "repos": [
        {
            "name": "Example OAI Repository",
            "url": "https://example-repository.ca/request",
            "homepage_url": "https://example-repository.ca",
            "set": "optional-set-name",
            "thumbnail": "https://example-repository.ca/logo.png",
            "type": "oai",
            "update_log_after_numitems": 50,
            "enabled": true
        },
        {
            "name": "Example CKAN Repository",
            "url": "https://example-repository.ca/data",
            "homepage_url": "https://example-repository.ca/",
            "set": "",
            "thumbnail": "https://example-repository.ca/logo.png",
            "type": "ckan",
            "repo_refresh_days": 7,
            "update_log_after_numitems": 2000,
            "item_url_pattern": "https://example-repository.ca/dataset/%id%",
            "enabled": true
        },
        {
            "name": "Example MarkLogic Repository",
            "url": "https://example-repository.ca/search",
            "homepage_url": "https://search2.example-repository.ca/",
            "item_url_pattern": "https://search2.example-repository.ca/#/details?uri=%2Fodesi%2F%id%",
            "thumbnail": "https://example-repository.ca/logo.png",
            "collection": "cora",
            "options": "odesi-opts2",
            "type": "marklogic",
            "enabled": true
        },
        {
            "name": "Example Socrata Repository",
            "url": "data.example-repository.ca",
            "homepage_url": "https://example-repository.ca",
            "set": "",
            "thumbnail": "https://example-repository.ca/logo.png",
            "item_url_pattern": "https://data.example-repository.ca/d/%id%",
            "type": "socrata",
            "enabled": true
        },
        {
            "name": "Example CSW Repository",
            "url": "https://example-repository.ca/geonetwork/srv/eng/csw",
            "homepage_url": "https://example-repository.ca",
            "item_url_pattern": "https://example-repository.ca/geonetwork/srv/eng/catalog.search#/metadata/%id%",
            "thumbnail": "https://example-repository.ca/logo.png",
            "type": "csw",
            "enabled": true
        },
        {
            "name": "Example OpenDataSoft Repository",
            "url": "https://example-repository.ca/api/datasets/1.0/search",
            "homepage_url": "example-repository.ca",
            "type": "opendatasoft",
            "thumbnail": "https://example-repository.ca/logo.png",
            "item_url_pattern": "https://example-repository.ca/explore/dataset/%id%",
            "enabled": true
        },
        {
            "name": "Example Dataverse Repository",
            "url": "https://example-repository.ca/api/dataverses/%id%/contents",
            "homepage_url": "example-repository.ca",
            "type": "dataverse",
            "enabled": true
        }
    ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right now, supported OAI metadata types are Dublin Core ("OAI-DC" is assumed by default and does not need to be specified) and FGDC.

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `globus_harvester.py`.

You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. There are two export formats which may be specified with the `--export-format` option: `dataverse` and `gmeta`. You can also use `--only-new-records` to only export records that have changed since the last run.

Supported database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support. Supported export formats in the config file are `gmeta` and `xml`; XML export requires the `dicttoxml` library.
