package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.updatebyquery.UpdateByQueryRequest;
import org.elasticsearch.action.updatebyquery.UpdateByQueryRequestBuilder;
import org.elasticsearch.action.updatebyquery.UpdateByQueryResponse;

public interface UpdateByQueryClient {

    /**
     * Updates documents that match a query specified in the request. The update is based on a script.
     *
     * @param request The update by query request.
     * @param listener A listener that notifies the caller when the update by query operation has completed
     */
    void updateByQuery(UpdateByQueryRequest request, ActionListener<UpdateByQueryResponse> listener);

    /**
     * Performs the same action as in {@link #updateByQuery(org.elasticsearch.action.updatebyquery.UpdateByQueryRequest,
     * org.elasticsearch.action.ActionListener)}, but works with an {@link ActionFuture} instead of a {@link ActionListener}.
     *
     * @param request The update query request
     * @return The result future
     */
    ActionFuture<UpdateByQueryResponse> updateByQuery(UpdateByQueryRequest request);

    /**
     * Prepares a update for documents matching a query using a script.
     *
     * @return a builder instance
     */
    UpdateByQueryRequestBuilder prepareUpdateByQuery();

}
