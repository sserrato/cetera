# Cetera Docker #

## Building ##

### Pre-Requisites ###
Both
[`sbt`](http://www.scala-sbt.org/0.13/tutorial/Installing-sbt-on-Linux.html)
and a
[JDK](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html)
(oracle-jdk-8 recommended)
must be installed to build.

To build the image, run:
```
cd .. # Change to project root.
sbt assembly
cp target/scala-2.10/cetera-assembly*.jar docker/cetera-assembly.jar
(sudo) docker build --rm -t cetera docker
```

## Running ##

An example of running against a completely local setup is:

```
echo 'ES_SERVER="localhost"' >> envfile
echo 'ES_PORT=9300' >> envfile
echo 'ES_CLUSTER_NAME="catalog"' >> envfile
echo 'MIN_SHOULD_MATCH="3<60%"' >> envfile
echo 'TITLE_BOOST=5.0' >> envfile
echo 'FUNCTION_SCORE_SCRIPTS=["views","score"]' >> envfile
echo 'METRICS_DIR="/tmp/metrics"' >> envfile

(sudo) docker run --env-file=envfile -it cetera
```

If you have problems parsing the config file, please check that the quoting
is as specified above.

## Required Environment Variables ##
* ES_SERVER - the hostname of elasticsearch's load balancer
* ES_PORT - the port for elasticsearch's load balancer, usually 80
* ES_CLUSTER_NAME - the cluster name for discovery's elasticsearch cluster
* MIN_SHOULD_MATCH - a [minimum_should_match](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-minimum-should-match.html) elasticsearch param.
* TITLE_BOOST - a positive float that boosts title matches
* FUNCTION_SCORE_SCRIPTS - an array of [function score scripts](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html); the only options to date are "views" and "score" 
* METRICS_DIR - optionally, the directory in which balboa metrics will be written; the base image defaults this to `/data/metrics`
