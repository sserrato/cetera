Cetera -- the golden-throated search service.
Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 5704.

Run it: `sbt run`

We support two endpoints:
* `curl localhost:5704/version` returns the version
* `curl localhost:5704/catalog` returns a stubbed response

Please see the [Apiary API spec](http://docs.ceterasearchservice.apiary.io/#) for API details.
