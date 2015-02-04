Cetera -- the golden-throated search service.
Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

Run it: `sbt run`

We support two endpoints:
* `curl localhost:5704/version` returns the version
* `curl localhost:5704/catalog` returns a stubbed response

Any other url gives an error message.

#API Spec
This is the API as specified. Features may not be implemented yet; however, we wish to future proof the API against version changes.
For all, the flags `verbose` and `highlight` should be set to `true` for the frontend, although they may be ignored for now.
Once we understand the behavior for the civic hacker, we will optimize the use case to return the appropriate, less verbose payload.
Given that our internal resource names will change, we will require external documentation that shares the resource that a civic hacker can use to load a dataset or page from its id.

## Querying all datasets and pages within the platform

```
GET /catalog/v1?verbose=true&highlight=true
```


## Restricting your query to a particular domain (cname)

```
GET /catalog/v1/cname?verbose=true&highlight=true
```


## Paging, Predefined facets (categories and agency) and restricting results to either pages or datasets
In general, refinement and search will be case-insensitive.

```
GET /catalog/v1/theDomain?categories=theCategory&from=74&size=1000&agency=theAgency&only=datasets&highlight=true
```
```
GET /catalog/v1/theDomain?categories=theCategory&from=74&size=1000&agency=theAgency&only=pages&highlight=true
```
The caller may pass in multiple categories, which are considered to have *disjunctive* (*inclusive*) semantics.
```
GET /catalog/v1/theDomain?categories=theCategory1,theCategory2&highlight=true
```
will return all datasets and pages in the union of category *theCategory1* and category *theCategory2*.


## Return body
All calls will return a JSON object containing the list of datasets and pages in the relevance that is appropriate to the query in a field called `results`. This `results` array contains an array of hashes. Each hash contains a key `resource` containing the resource to be returned. Hashes may contain additional fields to `resource` including metadata about the resource. Each resource will be either a page or a dataset.

Pages and datasets may be distinguished by the `type` field. This is set to `dataset` for datasets and to `page` for pages. Additionally, the bodies differ from each other as follows:

Pages include the fields `page_id`, `datasetId`, and `cards`

Datasets include the fields `id` and `columns`

```
{
  "results": [
    {
      "resource": {
        "columns": [
          {
            "cardinality": 1,
            "importance": 3,
            "logicalDatatype": "category",
            "name": "location_state",
            "physicalDatatype": "text",
            "title": "Location (state)"
          },
          {
            "cardinality": 15,
            "description": "City for the Public Library",
            "importance": 3,
            "logicalDatatype": "category",
            "name": "city",
            "physicalDatatype": "text",
            "title": "CITY"
          },
          {
            "cardinality": 23,
            "description": "5 digit postal Zip Code for the Public Library",
            "importance": 3,
            "logicalDatatype": "category",
            "name": "zipcode",
            "physicalDatatype": "number",
            "title": "ZIPCODE"
          }
        ],
        "defaultAggregateColumn": "defaultAggregateColumn",
        "defaultPage": "5b5c-jj6z",
        "description": "Locations of all Montgomery County, MD Public Libraries.",
        "domain": "moco-migrationtest.demo.socrata.com",
        "id": "xqvj-wiwq",
        "name": "Libraries",
        "ownerId": "h6pt-apgn",
        "rowDisplayUnit": "row",
        "type": "dataset",
        "updatedAt": "2015-01-06T16:59:37.000-08:00"
      }
    },
    {
      "resource": {
        "cards": [
          {
            "activeFilters": [],
            "cardCustomStyle": {},
            "cardSize": 2,
            "displayMode": "visualization",
            "expanded": false,
            "expandedCustomStyle": {},
            "fieldName": ":@computed_region_p3v4_2swa"
          }
        ],
        "datasetId": "xqvj-wiwq",
        "description": "Locations of all Montgomery County, MD Public Libraries.",
        "name": "Libraries",
        "pageId": "5b5c-jj6z",
        "rowDisplayUnit": "Row",
        "type": "page"
      }
    }
  ]
}
```
