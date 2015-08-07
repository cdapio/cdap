/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.template.etl.realtime;

import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.template.etl.realtime.sink.RealtimeCubeSink;
import co.cask.cdap.template.etl.realtime.sink.RealtimeElasticsearchSink;
import co.cask.cdap.template.etl.realtime.sink.RealtimeTableSink;
import co.cask.cdap.template.etl.realtime.sink.StreamSink;
import co.cask.cdap.template.etl.realtime.source.DataGeneratorSource;
import co.cask.cdap.template.etl.realtime.source.JmsSource;
import co.cask.cdap.template.etl.realtime.source.KafkaSource;
import co.cask.cdap.template.etl.realtime.source.SqsSource;
import co.cask.cdap.template.etl.realtime.source.TwitterSource;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.template.etl.transform.ScriptFilterTransform;
import co.cask.cdap.template.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Test for {@link RealtimeElasticsearchSink}
 */
public class RealtimeElasticsearchSinkTest extends TestBase {
  private static final Id.Namespace NAMESPACE = Id.Namespace.DEFAULT;
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLRealtime");
  private static final Gson GSON = new Gson();

  private Client client;
  private Node node;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void beforeTest() throws Exception {
    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
      .put("path.data", tmpFolder.newFolder("data"))
      .put("cluster.name", "testcluster");
    node = nodeBuilder().settings(elasticsearchSettings.build()).client(false).node();
    client = node.client();
  }

  @After
  public void afterTest() {
    node.close();
  }

  @BeforeClass
  public static void setupTests() throws IOException {
    // TODO: should only deploy test source and Elasticsearch sink (JIRA CDAP-3322)
    addTemplatePlugins(TEMPLATE_ID, "realtime-sources-1.0.0.jar",
                       DataGeneratorSource.class, JmsSource.class, KafkaSource.class,
                       TwitterSource.class, SqsSource.class);
    addTemplatePlugins(TEMPLATE_ID, "realtime-sinks-1.0.0.jar",
                       RealtimeCubeSink.class, RealtimeTableSink.class,
                       StreamSink.class, RealtimeElasticsearchSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar",
                       ProjectionTransform.class, ScriptFilterTransform.class,
                       StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLRealtimeTemplate.class,
                   PipelineConfigurable.class.getPackage().getName(),
                   ETLStage.class.getPackage().getName(),
                   RealtimeSource.class.getPackage().getName());
  }

  @Test
  @Category(SlowTests.class)
  public void testESSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                                                                    DataGeneratorSource.TABLE_TYPE));
    try {
      ETLStage sink = new ETLStage("Elasticsearch",
                                   ImmutableMap.of(Properties.Elasticsearch.TRANSPORT_ADDRESSES,
                                                   InetAddress.getLocalHost().getHostName() + ":9300",
                                                   Properties.Elasticsearch.CLUSTER, "testcluster",
                                                   Properties.Elasticsearch.INDEX_NAME, "test",
                                                   Properties.Elasticsearch.TYPE_NAME, "testing",
                                                   Properties.Elasticsearch.ID_FIELD, "name"
                                   ));
      List<ETLStage> transforms = new ArrayList<>();
      ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);
      Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "esSinkTest");
      AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
      AdapterManager adapterManager = createAdapter(adapterId, adapterConfig);

      adapterManager.start();
      TimeUnit.SECONDS.sleep(5);
      adapterManager.stop();

      SearchResponse searchResponse = client.prepareSearch("Bob").execute().actionGet();
      Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
      SearchHit result = searchResponse.getHits().getAt(0);

      Assert.assertEquals(1, (int) result.field("id").getValue());
      Assert.assertEquals("Bob", result.field("name").getValue().toString());
      Assert.assertEquals(3.4, (double) result.field("score").getValue(), 0.000001);
      Assert.assertEquals(false, result.field("graduated").getValue());
      Assert.assertNotNull(result.field("time").getValue());

      searchResponse = client.prepareSearch().setQuery(matchQuery("score", "3.4")).execute().actionGet();
      Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
      result = searchResponse.getHits().getAt(0);
      Assert.assertEquals("test", result.getIndex());
      Assert.assertEquals("testing", result.getType());
      Assert.assertEquals("Bob", result.getId());
      searchResponse = client.prepareSearch().setQuery(matchQuery("name", "ABCD")).execute().actionGet();
      Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

      DeleteResponse response = client.prepareDelete("test", "testing", "Bob").execute().actionGet();
      Assert.assertTrue(response.isFound());
    } finally {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("test")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    }
  }
}
