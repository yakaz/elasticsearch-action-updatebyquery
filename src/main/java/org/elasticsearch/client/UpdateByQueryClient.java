package org.elasticsearch.client;

public class UpdateByQueryClient implements Client {

    protected final Client client;

    public UpdateByQueryClient(Client client) {
        this.client = client;
    }


}
