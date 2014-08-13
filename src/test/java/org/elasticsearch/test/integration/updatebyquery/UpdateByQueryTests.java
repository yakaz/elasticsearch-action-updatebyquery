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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.updatebyquery.BulkResponseOption;
import org.elasticsearch.action.updatebyquery.IndexUpdateByQueryResponse;
import org.elasticsearch.action.updatebyquery.UpdateByQueryResponse;
import org.elasticsearch.client.UpdateByQueryClientWrapper;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.AliasAction.newAddAliasAction;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

@ClusterScope(
        scope = Scope.SUITE,      // needed to control node settings, in order to add "plugins.load_classpath_plugins=true"
        transportClientRatio = 0  // as we can't control the transport node settings to add "plugins.load_classpath_plugins=true", forbid TransportClients
)
public class UpdateByQueryTests extends ElasticsearchIntegrationTest {

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
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put("action.updatebyquery.bulk_size", 5)
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Test
    public void testUpdateByQuery() throws Exception {
        createIndex("test");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        final long numDocs = 25;
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", 1).execute().actionGet();
            if (i % 10 == 0) {
                client().admin().indices().prepareFlush("test").execute().actionGet();
            }
        }
        // Add one doc with a different type.
        client().prepareIndex("test", "type2", "1").setSource("field1", 1).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        CountResponse countResponse = client().prepareCount("test")
                .setQuery(termQuery("field1", 2)).get();
        assertThat(countResponse.getCount(), equalTo(0L));

        Map<String, Object> scriptParams = new HashMap<String, Object>();
        UpdateByQueryResponse response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("test")
                .setTypes("type1")
                .setIncludeBulkResponses(BulkResponseOption.ALL)
                .setScript("ctx._source.field1 += 1").setScriptParams(scriptParams)
                .setQuery(matchAllQuery())
                .execute()
                .actionGet();

        assertThat(response, notNullValue());
        assertThat(response.mainFailures().length, equalTo(0));
        assertThat(response.totalHits(), equalTo(numDocs));
        assertThat(response.updated(), equalTo(numDocs));
        assertThat(response.indexResponses().length, equalTo(1));
        assertThat(response.indexResponses()[0].countShardResponses(), equalTo(numDocs));

        assertThat(response.indexResponses()[0].failuresByShard().isEmpty(), equalTo(true));
        for (BulkItemResponse[] shardResponses : response.indexResponses()[0].responsesByShard().values()) {
            for (BulkItemResponse shardResponse : shardResponses) {
                assertThat(shardResponse.getVersion(), equalTo(2L));
                assertThat(shardResponse.isFailed(), equalTo(false));
                assertThat(shardResponse.getFailure(), nullValue());
                assertThat(shardResponse.getFailureMessage(), nullValue());
            }
        }

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        countResponse = client().prepareCount("test")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(numDocs));

        response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("test")
                .setTypes("type1")
                .setScript("ctx._source.field1 += 1").setScriptParams(scriptParams)
                .setQuery(matchAllQuery())
                .execute()
                .actionGet();

        assertThat(response, notNullValue());
        assertThat(response.totalHits(), equalTo(numDocs));
        assertThat(response.updated(), equalTo(numDocs));
        assertThat(response.indexResponses().length, equalTo(1));
        assertThat(response.indexResponses()[0].totalHits(), equalTo(numDocs));
        assertThat(response.indexResponses()[0].updated(), equalTo(numDocs));
        assertThat(response.indexResponses()[0].failuresByShard().size(), equalTo(0));
        assertThat(response.indexResponses()[0].responsesByShard().size(), equalTo(0));

        client().admin().indices().prepareRefresh("test").execute().actionGet();
        countResponse = client().prepareCount("test")
                .setQuery(termQuery("field1", 3))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(numDocs));
    }

    @Test
    public void testUpdateByQuery_multipleIndices() throws Exception {
        final int numIndices = 10;
        final long docsPerIndex = 10;
        final long numDocs = numIndices * docsPerIndex;

        // Create all indices beforehand
        for (int i = 0; i < numIndices; i++) {
            createIndex("test" + i);
            if (i % 5 == 0) {
                client().admin().indices().prepareFlush("test" + i).execute().actionGet();
            }
        }
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // Index docs
        for (int i = 0; i < numIndices; i++) {
            String current = "test" + i;
            for (int id = 0; id < docsPerIndex; ++id) {
                client().prepareIndex(current, "type1", Integer.toString(id)).setSource("field1", 1).execute().actionGet();
            }
        }
        // Add one doc with a different type.
        client().prepareIndex("test0", "type2", "-1").setSource("field1", 1).execute().actionGet();
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
        assertThat(response.indexResponses().length, equalTo(numIndices));
        Arrays.sort(response.indexResponses(), new Comparator<IndexUpdateByQueryResponse>() {

            public int compare(IndexUpdateByQueryResponse res1, IndexUpdateByQueryResponse res2) {
                int index1 = Integer.parseInt(res1.index().substring(4));
                int index2 = Integer.parseInt(res2.index().substring(4));
                return index1 - index2;
            }

        });

        for (int i = 0; i < response.indexResponses().length; i++) {
            String index = "test" + i;
            assertThat(response.indexResponses()[i].index(), equalTo(index));
            assertThat(response.indexResponses()[i].countShardResponses(), equalTo(docsPerIndex));

            assertThat(response.indexResponses()[i].failuresByShard().isEmpty(), equalTo(true));
            for (BulkItemResponse[] shardResponses : response.indexResponses()[i].responsesByShard().values()) {
                for (BulkItemResponse shardResponse : shardResponses) {
                    assertThat(shardResponse.getVersion(), equalTo(2L));
                    assertThat(shardResponse.isFailed(), equalTo(false));
                    assertThat(shardResponse.getFailure(), nullValue());
                    assertThat(shardResponse.getFailureMessage(), nullValue());
                }
            }
        }

        assertThat(response.mainFailures().length, equalTo(0));

        client().admin().indices().prepareRefresh("*").execute().actionGet();
        countResponse = client().prepareCount("*")
                .setQuery(termQuery("field1", 2))
                .execute()
                .actionGet();
        assertThat(countResponse.getCount(), equalTo(numDocs));
    }

    @Test
    public void testUpdateByQuery_usingAliases() {
        client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.builder()
                        .put(indexSettings())
                        .put("number_of_shards", Math.max(2, numberOfShards()))
                        .build()
        ).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client().admin().indices().prepareAliases().addAliasAction(
                newAddAliasAction("test", "alias0").routing("0")
        ).execute().actionGet();

        client().admin().indices().prepareAliases().addAliasAction(
                newAddAliasAction("test", "alias1").filter(FilterBuilders.termFilter("field", "value2")).routing("1")
        ).execute().actionGet();

        client().prepareIndex("alias0", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("alias0", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
        client().admin().indices().prepareFlush("test").execute().actionGet();
        client().prepareIndex("alias1", "type1", "3").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client().prepareIndex("alias1", "type1", "4").setSource("field", "value2").setRefresh(true).execute().actionGet();

        assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        assertThat(client().prepareGet("alias0", "type1", "2").execute().actionGet().isExists(), equalTo(true));
        assertThat(client().prepareGet("alias1", "type1", "3").execute().actionGet().isExists(), equalTo(true));
        assertThat(client().prepareGet("alias1", "type1", "4").execute().actionGet().isExists(), equalTo(true));

        UpdateByQueryResponse response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("alias1")
                .setQuery(matchAllQuery())
                .setScript("ctx.op = \"delete\"")
                .execute().actionGet();
        assertThat(response.totalHits(), equalTo(1L));
        assertThat(response.updated(), equalTo(1L));

        response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("alias0")
                .setQuery(matchAllQuery())
                .setScript("ctx.op = \"delete\"")
                .execute().actionGet();
        assertThat(response.totalHits(), equalTo(2L));
        assertThat(response.updated(), equalTo(2L));

        assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        assertThat(client().prepareGet("alias0", "type1", "2").execute().actionGet().isExists(), equalTo(false));
        assertThat(client().prepareGet("alias1", "type1", "3").execute().actionGet().isExists(), equalTo(true));
        assertThat(client().prepareGet("alias1", "type1", "4").execute().actionGet().isExists(), equalTo(false));
    }

    @Test
    public void testUpdateByQuery_noMatches() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "type1", "1").setSource("field1", 1).execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        CountResponse countResponse = client().prepareCount("test")
                .setQuery(termQuery("field2", 1)).get();
        assertHitCount(countResponse, 0);

        Map<String, Object> scriptParams = new HashMap<String, Object>();
        UpdateByQueryResponse response = updateByQueryClient().prepareUpdateByQuery()
                .setIndices("test")
                .setTypes("type1")
                .setIncludeBulkResponses(BulkResponseOption.ALL)
                .setScript("ctx._source.field1 += 1").setScriptParams(scriptParams)
                .setQuery(termQuery("field2", 1))
                .execute()
                .actionGet();

        assertThat(response, notNullValue());
        assertThat(response.mainFailures().length, equalTo(0));
        assertThat(response.totalHits(), equalTo(0l));
        assertThat(response.updated(), equalTo(0l));
        assertThat(response.indexResponses(), arrayWithSize(1));
        assertThat(response.indexResponses()[0].responsesByShard().isEmpty(), is(true));
    }

}
