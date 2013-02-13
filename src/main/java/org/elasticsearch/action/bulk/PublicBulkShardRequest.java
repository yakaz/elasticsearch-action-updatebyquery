package org.elasticsearch.action.bulk;

import org.elasticsearch.index.shard.ShardId;

public class PublicBulkShardRequest extends BulkShardRequest {

    public PublicBulkShardRequest() {
    }

    public PublicBulkShardRequest(String index, int shardId, boolean refresh, BulkItemRequest[] items) {
        super(index, shardId, refresh, items);
    }

}
