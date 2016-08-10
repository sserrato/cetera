# Cetera

[![Codacy Badge](https://api.codacy.com/project/badge/40da0e5a1758402cbae610a091659375)](https://www.codacy.com/app/johnkrah/cetera)

Cetera -- the golden-throated search service.

Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

## API

Check out [Cetera's Apiary spec](http://docs.cetera.apiary.io/#) for the latest API info.

## Run locally
`sbt cetera-http/run`

## Dependencies

##### Java 1.8

We recommend using [jEnv](http://www.jenv.be/) to manage your java versions. jEnv is a bit like
rbenv; it uses shims to run the chosen java version for your application. jenv
will read the .java-version file from Cetera's root directory and correctly
run Java 1.8.

##### Scala 2.11.7

Scala 2.11.7 is the current (as of 2015-01-07) 2.11.x release.

Scala 2.12 targets Java 8 only, and offers a number of useful features (not the
least of which is better tab completion in the Scala REPL). We would like to
upgrade to Scala 2.12 when this becomes possible.

## Configuration
In `configs`, you'll find `sample-cetera.conf`, which contains many of the parameters you should need to run
cetera locally.

We recommend you create a `local-cetera.conf`, which is gitignored. This is where you'll add additional parameters.
To use authentication, you'll need a `core` key with `host`, `appToken`, and `port` values.
Additionally, Socrata employees will need to be on the VPN to access cetera in any environments besides local.

## Elasticsearch setup

Cetera, in development use, assumes an ElasticSearch setup as follows:

- Java version: 1.8.0
- ES version: 1.7.2
- Host: localhost (127.0.0.1)
- Port: 9200 for HTTP requests and 9300 for Transport (java access)
- Cluster name: catalog

Assuming ES was installed with Homebrew, set the cluster name appropriatetely in `/usr/local/opt/elasticsearch/config/elasticsearch.yml`.

For non-homebrew installations, please find and edit this file.

### Install on Mac OS X with Homebrew

`brew install elasticsearch`

In `/usr/local/opt/elasticsearch/config/elasticsearch.yml` set `cluster.name: catalog`

`ln -sfv /usr/local/opt/elasticsearch/*.plist ~/Library/LaunchAgents` (optional, launch automatically)

`launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist` (launch now, do this outside of tmux)

### For non-Homebrew installations

Please find your elasticsearch.yml file, set `cluster.name: catalog`, and launch as appropriate.
