package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.updatebyquery.UpdateByQueryAction;
import org.elasticsearch.action.updatebyquery.UpdateByQueryRequest;
import org.elasticsearch.action.updatebyquery.UpdateByQueryRequestBuilder;
import org.elasticsearch.action.updatebyquery.UpdateByQueryResponse;

public class UpdateByQueryClientWrapper implements UpdateByQueryClient {

    protected final Client client;

    public UpdateByQueryClientWrapper(Client client) {
        this.client = client;
    }

    @Override
    public void updateByQuery(UpdateByQueryRequest request, ActionListener<UpdateByQueryResponse> listener) {
        client.execute(UpdateByQueryAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<UpdateByQueryResponse> updateByQuery(UpdateByQueryRequest request) {
        return client.execute(UpdateByQueryAction.INSTANCE, request);
    }

    @Override
    public UpdateByQueryRequestBuilder prepareUpdateByQuery() {
        return new UpdateByQueryRequestBuilder(client);
    }

}
