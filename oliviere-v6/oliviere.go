package oliviere_v6

/** oliviere_v6 is a helper to identify the shard and server on which a document exists.

It can be used to group together large BulkRequests in Bulks that hit only a specific shard.
This should be faster because the coordinator role will be simplified, as each bulk only hits one server and one shard.

This is the implementation that uses https://godoc.org/github.com/olivere/elastic v6 structures
The algorithm can be replicated for any other ES driver.

https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html
shard_num = hash(_routing) % NumPrimaryShards
shard_num = (hash(_routing) + hash(_id) % RoutingPartitionSize) % NumPrimaryShards

*/

import (
	"context"
	"errors"

	"github.com/bgadrian/es-bulk-shards/routing"

	"github.com/olivere/elastic/v6"
)

type IndexSettings struct {
	RoutingPartitionSize int `json:"routing_partition_size"`
	NumPrimaryShards     int `json:"number_of_shards"`
}

// Router identifies the shard number on which a specific routingKey should exists
type Router struct {
	cache  map[string]IndexSettings
	client elastic.Client
}

func NewRouter(client elastic.Client) *Router {
	return &Router{
		cache:  make(map[string]IndexSettings),
		client: client,
	}
}

// ShardNum returns the shardIndex for a specific document.
func (r *Router) ShardNum(ctx context.Context, indexName, id, docID, docRouting string) (int, error) {
	sett, isCached := r.cache[indexName]
	if !isCached {
		s, err := r.fetchIndexSettings(ctx, indexName)
		if err != nil {
			return 0, err
		}
		sett = s
	}

	hashRouting, err := routing.Murmur3HashFunction(docRouting)
	if err != nil {
		return 0, err
	}

	if sett.RoutingPartitionSize > 1 {
		//shard_num = (hash(_routing) + hash(_id) % RoutingPartitionSize) % NumPrimaryShards
		hashID, err := routing.Murmur3HashFunction(docID)
		if err != nil {
			return 0, err
		}
		return (hashRouting + hashID%sett.RoutingPartitionSize) % sett.NumPrimaryShards, nil
	}
	//shard_num = hash(_routing) % NumPrimaryShards
	return hashRouting % sett.NumPrimaryShards, nil
}

func (r *Router) fetchIndexSettings(ctx context.Context, indexName string) (IndexSettings, error) {
	data := IndexSettings{
		//default values from docs
		RoutingPartitionSize: 1,
		NumPrimaryShards:     5,
	}

	// https://www.elastic.co/guide/en/elasticsearch/reference/6.8/index-modules.html
	all, err := r.client.IndexGetSettings(indexName).
		Name("number_of_shards", "routing_partition_size").
		Do(ctx)
	if err != nil {
		return data, err
	}
	sett, haveResponse := all[indexName]
	if !haveResponse || sett == nil || sett.Settings == nil {
		return data, errors.New("missing IndexSettings")
	}

	if val, hasData := sett.Settings["number_of_shards"]; hasData && val != nil {
		asInt, ok := val.(int)
		if ok {
			data.NumPrimaryShards = asInt
		}
	}

	if val, hasData := sett.Settings["routing_partition_size"]; hasData && val != nil {
		asInt, ok := val.(int)
		if ok {
			data.RoutingPartitionSize = asInt
		}
	}
	return data, nil
}
