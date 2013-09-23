/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * Represents an update by query request targeted for a specific index.
 */
public class IndexUpdateByQueryRequest extends IndexReplicationOperationRequest {

    private String[] types = new String[0];
    private BulkResponseOption bulkResponseOption;
    private String[] filteringAliases = new String[0];
    private Set<String> routing = new HashSet();

    private BytesReference source;
    private boolean sourceUnsafe;

    IndexUpdateByQueryRequest() {
    }

    IndexUpdateByQueryRequest(UpdateByQueryRequest request, String index, String[] filteringAliases, Set<String> routing) {
        this.replicationType = request.replicationType();
        this.consistencyLevel = request.consistencyLevel();
        this.timeout = request.timeout();
        this.listenerThreaded(request.listenerThreaded());
        this.index = index;
        this.types = request.types();
        this.bulkResponseOption = request.bulkResponseOptions();
        this.source = request.source();
        this.sourceUnsafe = request.sourceUnsafe();
        if (filteringAliases != null) {
            this.filteringAliases = filteringAliases;
        }
        if (routing != null) {
            this.routing = routing;
        }
    }

    public String[] types() {
        return types;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    public BulkResponseOption bulkResponseOptions() {
        return bulkResponseOption;
    }

    public Set<String> routing() {
        return routing;
    }

    public BytesReference source() {
        return source;
    }

    public boolean sourceUnsafe() {
        return sourceUnsafe;
    }

    public void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("Source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        String[] readStringArray = in.readStringArray();
		types = readStringArray;
        bulkResponseOption = BulkResponseOption.fromId(in.readByte());
        filteringAliases = readStringArray;
        routing = new HashSet();
        routing.addAll(Arrays.asList(readStringArray));
        source = in.readBytesReference();
        sourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(types);
        out.writeByte(bulkResponseOption.id());
        out.writeStringArray(filteringAliases);
        out.writeStringArray(routing.toArray(new String[routing.size()]));
        out.writeBytesReference(source);
    }

}
