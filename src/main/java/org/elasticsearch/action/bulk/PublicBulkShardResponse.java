package org.elasticsearch.action.bulk;

import org.elasticsearch.index.shard.ShardId;

public class PublicBulkShardResponse extends BulkShardResponse {

    public PublicBulkShardResponse() {
    }

    public PublicBulkShardResponse(ShardId shardId, BulkItemResponse[] responses) {
        super(shardId, responses);
    }

}
