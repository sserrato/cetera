Cetera -- the golden-throated search service.
Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

Run it: `sbt run`

We support two endpoints:
* `curl localhost:5704/version` returns the version
* `curl localhost:5704/catalog` returns a stubbed response

Any other url gives an error message.

# ElasticSearch setup

Cetera, in development use, assumes an ElasticSearch setup as follows:

- ES version: 1.4.2
- Host: localhost (127.0.0.1)
- Port: 9200 for HTTP requests and 9300 for Transport (java access)
- Cluster name: catalog

Assuming ES was installed with Homebrew, set the cluster name appropriatetely in `/usr/local/opt/elasticsearch/config/elasticsearch.yml`. For non-homebrew installations, please find and edit this file.

# API Spec

Please see the [Apiary API spec](http://docs.cetera.apiary.io/#) for API details.
