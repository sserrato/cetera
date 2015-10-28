# Cetera --<br/>The Golden-Throated Search Service.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-generate-toc again -->
**Table of Contents**

- [Cetera --<br/>The Golden-Throated Search Service.](#cetera---brthe-golden-throated-search-service)
    - [API Spec](#api-spec)
    - [Basic Endpoints](#basic-endpoints)
        - [`/catalog`](#catalog)
        - [`/catalog/categories`](#catalogcategories)
        - [`/catalog/tags`](#catalogtags)
        - [`/catalog/domains`](#catalogdomains)
        - [`/catalog/domains/www.example.com`](#catalogdomainswwwexamplecom)
        - [`/catalog/domains/www.example.com/facets`](#catalogdomainswwwexamplecomfacets)
        - [`/catalog/domains/www.example.com/facets/categories`](#catalogdomainswwwexamplecomfacetscategories)
        - [`/catalog/domains/www.example.com/facets/tags`](#catalogdomainswwwexamplecomfacetstags)
        - [`/catalog/domains/www.example.com/facets/metadata`](#catalogdomainswwwexamplecomfacetsmetadata)
        - [`/version`](#version)
    - [Quickstart](#quickstart)
        - [So how is this useful as an OS project? Who would possibly want to look at this?](#so-how-is-this-useful-as-an-os-project-who-would-possibly-want-to-look-at-this)
        - [Run natively](#run-natively)
        - [Run virtually (NOTE: boot2docker is deprecated)](#run-virtually-note-boot2docker-is-deprecated)
        - [Additional Notes for boot2docker](#additional-notes-for-boot2docker)
            - [Initialize a boot2docker VM](#initialize-a-boot2docker-vm)
            - [Mount the appropriate folder(s)](#mount-the-appropriate-folders)
- [Elasticsearch setup](#elasticsearch-setup)
    - [Install on Mac OS X with Homebrew](#install-on-mac-os-x-with-homebrew)
    - [For non-Homebrew installations](#for-non-homebrew-installations)

<!-- markdown-toc end -->

A wrapper around elasticsearch to enable searching, sorting, and filtering of
Socrata's catalog of datasets and _other things_.

By default, Cetera runs on port 5704. Ask a numerologist.

## Who is this for?

TODO: move the Quickstart subsection up here

## API Spec

See the [Apiary API spec](http://docs.cetera.apiary.io/#) for API details.
Cetera is still beta; not all features in the API are implemented, and many
implemented features are not in the API. But there is some intersect.

## Basic Endpoints

Here are the basic endpoints (all GETs):
* /catalog
* /catalog/categories
* /catalog/tags
* /catalog/domains
* /catalog/domains/_www.example.com_
* /catalog/domains/_www.example.com_/facets
* /catalog/domains/_www.example.com_/facets/categories
* /catalog/domains/_www.example.com_/facets/tags
* /catalog/domains/_www.example.com_/facets/metadata
* /version 
* /health

You may specify the version if you like: `/catalog/v1?q=driver`

If you do not specify a version, we default to the most recent.

There is only one version.

Examples (running locally):
* `curl localhost:5704/version` # get the version
* `curl localhost:5704/health` # get the health
* `curl localhost:5704/catalog\?q=search%20terms` # search for "search terms"
* `curl localhost:5704/catalog/domains` # how many documents per domain
* `curl localhost:5704/catalog/tags\?q=driver ` # get tag counts for documents matching "driver"


### `/catalog`

This is the main endpoint for searching. It accepts all kinds of parameters:
* `q` search query (if present, sort order is relevance * log(total\_page\_views))
* `domains` restrict search to csv list of domains
* _`some_example_metadata_field`_ restrict search to matching on a domain's custom metadata (requires `search_context`) ???
* `search_context` use this domain's set of custom categories, tags, and metadata
* `categories` restrict search to csv list of categories, either Socrata-defined `categories` or the custom `domain_categories` if `search_context` is present
* `tags` restrict search to csv list of tags, either Socrata-defined `tags` or custom `domain_tags` if `search_context` is present
* `only` restrict search to a single type of document: _calendars, charts, data\_lenses (also datalenses and lenses), datasets, external (deprecated), files, filters, forms, hrefs (same as links), lenses (same as data\_lenses), links (same as hrefs), maps._ Will accept both singular and plural forms. This is not good and we will probably decide on one or the other soon.

### `/catalog/categories`
returns a count query grouped by category

### `/catalog/tags`
returns a count query grouped by tag

### `/catalog/domains`

### `/catalog/domains/www.example.com`
is currently broken

### `/catalog/domains/www.example.com/facets`
available facets for that domain

### `/catalog/domains/www.example.com/facets/categories`
values and document counts for the a domain's 'tags' facet

### `/catalog/domains/www.example.com/facets/tags`
values and document counts for the a domain's 'tags' facet

### `/catalog/domains/www.example.com/facets/metadata`
values and document counts for the a domain's 'tags' facet

### `/version`
returns the version (not publicly accessible through api.us.socrata.com/catalog)


See the [Apiary API spec](http://docs.cetera.apiary.io/#) for more detail.

Any other url should give an error message.

## Quickstart

Cetera is a wrapper on an Elasticsearch cluster, so naturally you need an
Elasticsearch cluster with data in it in the schema (or mapping) that Cetera
expects. This schema is constantly changing and very messy. When it settles
down, we should document it.

### So how is this useful as an OS project? Who would possibly want to look at this?

Good question.

* If you are a civic hacker interacting with the
  [Cetera search service](http://api.us.socrata.com/api/catalog) and would like
  to read the source code to know exactly what's going on, here it is.

* If you want to write a wrapper around ES in Scala, Cetera is an example of
  just that. Elasticsearch is not especially well documented, and anything
  beyond getting started can be pretty difficult.

* If you want to use the
  [socrata-http](https://github.com/socrata-platform/socrata-http) http toolkit,
  Cetera is full of simple examples.

* If you are interested in the excellent but not super-popular-on-stack-overflow
  Scala JSON library [rojoma-json](https://github.com/rjmac/rojoma-json), Cetera
  contains examples of common use cases.

### Run natively
`sbt run`

### Run virtually (NOTE: boot2docker is deprecated)
* If you are not a boot2docker expert, check out these
  [notes](#additional-notes-for-boot2docker)
* From your local machine:
```
sbt assembly
cp target/scala-2.10/Cetera-assembly-0.1-SNAPSHOT.jar ./cetera-assembly.jar
boot2docker ssh
```
* Once inside the boot2docker VM:
```
docker build --rm -t cetera .
echo "ES_CLUSTER_NAME=<your_cluster_name>" > envfile
echo "ES_SERVER=<your_server_address>" >> envfile
echo "ES_PORT=9300" >> envfile
docker run --env-file=envfile -p 5704:5704 -i cetera
```

### Additional Notes for boot2docker

*NOTE:* boot2docker is deprecated in favor of
 [Docker Machine](https://docs.docker.com/machine/)

If you are using a Mac and boot2docker, you will not be able to run the above
commands without first initializing a boot2docker VM and mounting the
appropriate folders - in this case, just cetera.

#### Initialize a boot2docker VM
```
boot2docker init
boot2docker up
```

Running the `up` command may inform you of a number of environment variables
which must be set. You can copy the export commands into your shell or shell rc.
You can then rerun the `boot2docker up` command.

#### Mount the appropriate folder(s)
* Launch your VirtualBox manager
* Right-click on the boot2docker VM and choose settings
* From 'shared folders' click the `+` icon to add a folder
* Click the dropdown arrow on the folder-path select bar and select other
* Select the source directory of your project
* Select "Auto-mount" and "Make Permanent"

* Mount the folder(s) in the VM:
```
boot2docker ssh
mkdir cetera
sudo mount -t vboxsf -o uid=1000,gid=50 cetera cetera
```

Now you are ready to docker.

**BEWARE**: Changing anything in the **mounted** directory *will actually change
  it on your local filesystem*.


# Elasticsearch setup

Cetera, in development use, assumes an ElasticSearch setup as follows:

- Java version: 1.8.0 (yay!)
- ES version: 1.7.2 (needed for Java 1.8.0)
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
