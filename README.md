# cetera
Cetera -- the golden throated search service. Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for use by the front end in displaying search results.

By default, cetera runs on port 1944.

Run it: `sbt run`

We support one endpoint: `curl localhost:1944/version`

Any other url gives an error message.

