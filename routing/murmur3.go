package routing

import "github.com/spaolacci/murmur3"

/* hash replicates the behaviour of the ES Cluster Routing algorithms

The hash is used in the routing formulas since ES 2.0:
https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-routing-field.html
shard_num = hash(_routing) % num_primary_shards
shard_num = (hash(_routing) + hash(_id) % routing_partition_size) % num_primary_shards

For the original algorithm see https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/cluster/routing/Murmur3HashFunction.java

murmur3 used by ES
https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java
public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {
*/

// Murmur3HashFunction replicates the elasticsearch/cluster/routing/Murmur3HashFunction.java
func Murmur3HashFunction(routing string) (int, error) {
	bytesToHash := make([]byte, len(routing)*2)
	for i, c := range routing {
		b1 := byte(c)
		//we do not have sign on bytes from a string, so it should behave same as >>> in java
		b2 := byte(c >> 8)
		bytesToHash[i*2] = b1
		bytesToHash[i*2+1] = b2
	}

	//in java murmurhash3_x86_32 is called
	h := murmur3.New32WithSeed(0)
	_, err := h.Write(bytesToHash)
	if err != nil {
		return 0, err
	}
	return int(h.Sum32()), nil
}
