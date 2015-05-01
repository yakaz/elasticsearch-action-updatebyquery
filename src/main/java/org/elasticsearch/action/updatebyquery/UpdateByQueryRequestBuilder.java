/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.IndicesReplicationOperationRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.UpdateByQueryClientWrapper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.Map;

/**
 * A request builder that produces {@link IndexUpdateByQueryRequest} instances.
 */
public class UpdateByQueryRequestBuilder extends IndicesReplicationOperationRequestBuilder<UpdateByQueryRequest, UpdateByQueryResponse, UpdateByQueryRequestBuilder> {

    private final UpdateByQueryClientWrapper updateByQueryClientWrapper;
    private UpdateByQuerySourceBuilder sourceBuilder;

    public UpdateByQueryRequestBuilder(Client client) {
        super(client, new UpdateByQueryRequest());
        updateByQueryClientWrapper = new UpdateByQueryClientWrapper(client);
    }

    public UpdateByQueryRequestBuilder setTypes(String... types) {
        request().types(types);
        return this;
    }

    public UpdateByQueryRequestBuilder setIncludeBulkResponses(BulkResponseOption option) {
        request().bulkResponseOptions(option);
        return this;
    }

    public UpdateByQueryRequestBuilder setReplicationType(ReplicationType replicationType) {
        request().replicationType(replicationType);
        return this;
    }

    public UpdateByQueryRequestBuilder setConsistencyLevel(WriteConsistencyLevel writeConsistencyLevel) {
        request().consistencyLevel(writeConsistencyLevel);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public UpdateByQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     */
    public UpdateByQueryRequestBuilder setQuery(BytesReference query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * The language of the script to execute.
     * Valid options are: mvel, js, groovy, python, and native (Java)<br>
     * Default: groovy
     * <p>
     * Ref: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-scripting.html
     */
    public UpdateByQueryRequestBuilder setScriptLang(String lang) {
        sourceBuilder().scriptLang(lang);
        return this;
    }

    /**
     * The inline script to execute.
     * @see #setScript(String, ScriptType)
     */
    public UpdateByQueryRequestBuilder setScript(String script) {
        return setScript(script, ScriptType.INLINE);
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     * <p>
     * The script works with the variable <code>ctx</code>, which is bound to the entry,
     * e.g. <code>ctx._source.mycounter += 1</code>.
     *
     * @see #setScriptLang(String)
     * @see #setScriptParams(Map)
     */
    public UpdateByQueryRequestBuilder setScript(String script, ScriptType scriptType) {
        sourceBuilder().script(script, scriptType);
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     */
    public UpdateByQueryRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        if (scriptParams != null) {
            sourceBuilder().scriptParams(scriptParams);
        }
        return this;
    }

    /**
     * Add a script parameter.
     */
    public UpdateByQueryRequestBuilder addScriptParam(String name, String value) {
        sourceBuilder().addScriptParam(name, value);
        return this;
    }

    protected void doExecute(ActionListener<UpdateByQueryResponse> listener) {
        if (sourceBuilder != null) {
            request.source(sourceBuilder);
        }

        updateByQueryClientWrapper.updateByQuery(request, listener);
    }

    private UpdateByQuerySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new UpdateByQuerySourceBuilder();
        }
        return sourceBuilder;
    }

}
