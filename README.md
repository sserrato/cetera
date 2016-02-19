[![Codacy Badge](https://api.codacy.com/project/badge/40da0e5a1758402cbae610a091659375)](https://www.codacy.com/app/johnkrah/cetera)

Cetera -- the golden-throated search service.

Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

## Run locally
`sbt run`

## Endpoints!

We support a few endpoints:

* `/version` returns the version
* `/catalog` returns search results for query passed in parameter `q`
* `/catalog/domains` returns a count query grouped by domain
* `/catalog/categories` returns a count query grouped by category
* `/catalog/tags` returns a count query grouped by tag

You may specify the version if you like: `/catalog/v1?q=driver`

If you do not specify a version, we default to the most recent.

There is only one version.

Examples (running locally):
* `curl localhost:5704/version` # get the version
* `curl localhost:5704/catalog\?q=search%20terms` # search for "search terms"
* `curl localhost:5704/catalog/domains` # how many documents per domain
* `curl localhost:5704/catalog/tags\?q=driver ` # get tag counts for documents matching "driver"

See the [Apiary API spec](http://docs.cetera.apiary.io/#) for more detail.

# Elasticsearch setup

Cetera, in development use, assumes an ElasticSearch setup as follows:

- Java version: 1.8.0
- ES version: 1.7.2
- Host: localhost (127.0.0.1)
- Port: 9200 for HTTP requests and 9300 for Transport (java access)
- Cluster name: catalog

Assuming ES was installed with Homebrew, set the cluster name appropriatetely in `/usr/local/opt/elasticsearch/config/elasticsearch.yml`.

For non-homebrew installations, please find and edit this file.

## Install on Mac OS X with Homebrew

`brew install elasticsearch`

In `/usr/local/opt/elasticsearch/config/elasticsearch.yml` set `cluster.name: catalog`

`ln -sfv /usr/local/opt/elasticsearch/*.plist ~/Library/LaunchAgents` (optional, launch automatically)

`launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist` (launch now, do this outside of tmux)

## For non-Homebrew installations

Please find your elasticsearch.yml file, set `cluster.name: catalog`, and launch as appropriate.

# API Spec

Please see the [Apiary API spec](http://docs.cetera.apiary.io/#) for API details.
