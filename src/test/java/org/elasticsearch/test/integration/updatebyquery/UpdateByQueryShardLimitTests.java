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

package org.elasticsearch.test.integration.updatebyquery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.updatebyquery.BulkResponseOption;
import org.elasticsearch.action.updatebyquery.UpdateByQueryResponse;
import org.elasticsearch.client.UpdateByQueryClientWrapper;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(
        scope = Scope.SUITE,      // needed to control node settings, in order to add "plugins.load_classpath_plugins=true"
        transportClientRatio = 0  // as we can't control the transport node settings to add "plugins.load_classpath_plugins=true", forbid TransportClients
)
public class UpdateByQueryShardLimitTests extends ElasticsearchIntegrationTest {


    private final static int BULK_SIZE = 5;
    private final static int MAX_ITEMS_PER_SHARD = 5;

    public static UpdateByQueryClientWrapper updateByQueryClient() {
        return new UpdateByQueryClientWrapper(ElasticsearchIntegrationTest.client());
    }

    protected void createIndex(String indexName) throws Exception {
        logger.info("--> creating index " + indexName);
        prepareCreate(indexName).addMapping("type1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                .endObject()
                .endObject())
                .addMapping("subtype1", XContentFactory.jsonBuilder()
                .startObject()
                .startObject("subtype1")
                .startObject("_parent").field("type", "type1").endObject()
                .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                .endObject()
                        .endObject())
                .execute().actionGet();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("path.conf", this.getResource("config").getPath())
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put("action.updatebyquery.bulk_size", BULK_SIZE)
                .put("action.updatebyquery.max_items_per_shard", MAX_ITEMS_PER_SHARD)
                .put("script.disable_dynamic", false)
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }


    @Test
    public void testUpdateByQuery_matchedMoreThanLimit() throws Exception {
        final long numDocs = 100;
        final long maxItemsPerUpdateByQuery = MAX_ITEMS_PER_SHARD * numberOfShards();

        createIndex("test");
        client().admin().indices().prepareFlush("test").execute().actionGet();

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int id = 0; id < numDocs; ++id) {
            client().prepareIndex("test", "type1", Integer.toString(id)).setSource("field1", 1).execute().actionGet();
        }

        client().admin().indices().prepareRefresh("*").execute().actionGet();

        CountResponse countResponse = client().prepareCount("*")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(0L));

        Map<String, Object> scriptParams = new HashMap<String, Object>();
        UpdateByQueryResponse response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("*")
                .setTypes("type1")
                .setIncludeBulkResponses(BulkResponseOption.ALL)
                .setScript("ctx._source.field1 += 1").setScriptParams(scriptParams)
                .setQuery(matchAllQuery())
                .execute()
                .actionGet();

        assertThat(response, notNullValue());
        assertThat(response.totalHits(), equalTo(numDocs));
        assertThat(response.updated(), lessThanOrEqualTo(maxItemsPerUpdateByQuery));
        assertThat(response.mainFailures().length, equalTo(0));

        client().admin().indices().prepareRefresh("*").execute().actionGet();
        countResponse = client().prepareCount("*")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), lessThanOrEqualTo(maxItemsPerUpdateByQuery));
    }

    @Test
    public void testUpdateByQuery_matchedLessThanLimit() throws Exception {
        final long numDocs = MAX_ITEMS_PER_SHARD - 1;

        createIndex("test");
        client().admin().indices().prepareFlush("test").execute().actionGet();

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int id = 0; id < numDocs; ++id) {
            client().prepareIndex("test", "type1", Integer.toString(id)).setSource("field1", 1).execute().actionGet();
        }

        client().admin().indices().prepareRefresh("*").execute().actionGet();

        CountResponse countResponse = client().prepareCount("*")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(0L));

        Map<String, Object> scriptParams = new HashMap<String, Object>();
        UpdateByQueryResponse response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("*")
                .setTypes("type1")
                .setIncludeBulkResponses(BulkResponseOption.ALL)
                .setScript("ctx._source.field1 += 1").setScriptParams(scriptParams)
                .setQuery(matchAllQuery())
                .execute()
                .actionGet();

        assertThat(response, notNullValue());
        assertThat(response.totalHits(), equalTo(numDocs));
        assertThat(response.updated(), equalTo(numDocs));
        assertThat(response.mainFailures().length, equalTo(0));

        client().admin().indices().prepareRefresh("*").execute().actionGet();
        countResponse = client().prepareCount("*")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(numDocs));
    }

}
