Cetera -- the golden-throated search service.

Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

## Run natively
`sbt run`

## Run virtually
* If you are not a boot2docker expert, check out these [notes](#additional-notes-for-boot2docker)
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

If you are using a Mac and boot2docker, you will not be able to run the
above commands without first initializing a boot2docker VM and mounting the
appropriate folders - in this case, just cetera.

#### Initialize a boot2docker VM
```
boot2docker init
boot2docker up
```
Running the `up` command may inform you of a number of environment variables which must be set.
You can copy the export commands into your shell or shell rc.
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

**BEWARE**: Changing anything in the **mounted** directory *will actually change it on your local filesystem*.


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

Any other url gives an error message.


# Elasticsearch setup

Cetera, in development use, assumes an ElasticSearch setup as follows:

- ES version: 1.4.2
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
