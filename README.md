# ES bulk shards for Go

A little Elasticsearch helper to be used in high troughput write Go services. 
It is used when Bulks and large refresh_interval is not enough.

It can be used to group together large BulkRequests in Bulks that hit only a specific shard, by precalculating its shard number.
It fetches the indecs settings and replicates the ES Routing formulas on the client side.

This should be faster because the coordinator job will be simplified, as each bulk only hits one server and one shard.

It supports oliviere/v6 but support can be added for any other ES driver.