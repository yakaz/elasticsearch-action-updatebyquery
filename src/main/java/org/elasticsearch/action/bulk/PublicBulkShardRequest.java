package org.elasticsearch.action.bulk;

public class PublicBulkShardRequest extends BulkShardRequest {

    public PublicBulkShardRequest() {
    }

    public PublicBulkShardRequest(BulkRequest bulkRequest, String index, int shardId, boolean refresh, BulkItemRequest[] items) {
        super(bulkRequest, index, shardId, refresh, items);
    }

}
