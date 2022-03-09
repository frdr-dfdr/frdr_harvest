# FRDR Harvester

This is a repository crawler which outputs gmeta.json files for indexing by Globus. It currently supports harvesting with the following standards and APIs:

- Repositories using any of these platforms:
    - [Dataverse](https://dataverse.org/)
    - [CKAN](https://ckan.org/)
    - [Socrata](https://dev.socrata.com/)
    - [OpenDataSoft](https://www.opendatasoft.com/)
    - [ArcGIS](https://www.esri.com/en-us/arcgis/products/arcgis-open-data)
    - [MarkLogic](https://www.marklogic.com/)
    - [GeoNetwork](https://geonetwork-opensource.org/)
- Repositories that implement either of these standards:
    - [OAI-PMH](https://www.openarchives.org/pmh/)
- Repositories that register DOIs with [DataCite](https://datacite.org/).
- Repositories with a custom REST API, sitemap, or other means of providing machine-readable metadata, on a case-by-case basis.


## Configuration
Configuration is split into two files:

- The first controls the operation of the indexer, and is located in conf/harvester.conf.
- The list of repositories to be crawled is in conf/repos.json, structured as shown below. For documentation of repos.json properties, see: [repos\_json\_properties.md](https://github.com/frdr-dfdr/frdr_harvest/blob/master/admin/repos_json_properties.md).

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ json
{
    "repos": [
        {
            "name": "FRDR",
            "type": "oai",
            "url": "https://frdr-dfdr.ca/oai/request",
            "homepage_url": "https://frdr-dfdr.ca/",
            "thumbnail": "https://frdr-dfdr.ca/discover/img/sources/frdr_80x80.png",
            "set": "col_storagegroup1",
            "metadataprefix": "frdr",
            "repo_refresh_days": 0,
            "enabled": true
        },
        {
            "name": "UBC Dataverse",
            "type": "dataverse",
            "url": "https://dataverse.scholarsportal.info/api/dataverses/%id%/contents",
            "homepage_url": "https://dataverse.scholarsportal.info/dataverse/ubc",
            "thumbnail": "https://frdr-drdr.ca/discover/img/sources/ubccrest_80x80.png",
            "set": 71618,
            "enabled": true
        }
    ]
}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can call the crawler directly, which will run once, crawl all of the target domains, export metadata, and exit, by using `harvester.py`.

You can also run it with `--onlyharvest` or `--onlyexport` if you want to skip the metadata export or crawling stages, respectively. There are two export formats which may be specified with the `--export-format` option: `dataverse` and `gmeta`. You can also use `--only-new-records` to only export records that have changed since the last run.

Supported database types are "sqlite" and "postgres"; the `psycopg2` library is required for postgres support.