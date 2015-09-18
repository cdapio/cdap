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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.etl.realtime.sink.RealtimeElasticsearchSink;
import co.cask.cdap.etl.realtime.source.DataGeneratorSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.WorkerManager;
import com.google.common.collect.ImmutableMap;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Test for {@link RealtimeElasticsearchSink}
 */
public class RealtimeElasticsearchSinkTest extends ETLRealtimeBaseTest {
  private Client client;
  private Node node;
  private int port;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void beforeTest() throws Exception {
    port = Networks.getRandomPort();
    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
      .put("path.data", tmpFolder.newFolder("data"))
      .put("cluster.name", "testcluster")
      .put("transport.tcp.port", port);
    node = nodeBuilder().settings(elasticsearchSettings.build()).client(false).node();
    client = node.client();
  }

  @After
  public void afterTest() {
    node.close();
  }

  @Test
  @Category(SlowTests.class)
  public void testESSink() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                                                                    DataGeneratorSource.TABLE_TYPE));
    try {
      ETLStage sink = new ETLStage("Elasticsearch",
                                   ImmutableMap.of(Properties.Elasticsearch.TRANSPORT_ADDRESSES,
                                                   InetAddress.getLocalHost().getHostName() + ":" + port,
                                                   Properties.Elasticsearch.CLUSTER, "testcluster",
                                                   Properties.Elasticsearch.INDEX_NAME, "test",
                                                   Properties.Elasticsearch.TYPE_NAME, "testing",
                                                   Properties.Elasticsearch.ID_FIELD, "name"
                                   ));
      List<ETLStage> transforms = new ArrayList<>();
      ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, transforms);


      Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testCubeSink");
      AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
      ApplicationManager appManager = deployApplication(appId, appRequest);

      WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

      workerManager.start();
      Tasks.waitFor(1L, new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          try {
            SearchResponse searchResponse = client.prepareSearch("test").execute().actionGet();
            return searchResponse.getHits().getTotalHits();
          } catch (Exception e) {
            //the index test won't exist until the run is finished
            return 0L;
          }
        }
      }, 15, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
      workerManager.stop();

      SearchResponse searchResponse = client.prepareSearch("test").execute().actionGet();
      Map<String, Object> result = searchResponse.getHits().getAt(0).getSource();

      Assert.assertEquals(1, (int) result.get("id"));
      Assert.assertEquals("Bob", result.get("name"));
      Assert.assertEquals(3.4, (double) result.get("score"), 0.000001);
      Assert.assertEquals(false, result.get("graduated"));
      Assert.assertNotNull(result.get("time"));

      searchResponse = client.prepareSearch().setQuery(matchQuery("score", "3.4")).execute().actionGet();
      Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
      SearchHit hit = searchResponse.getHits().getAt(0);
      Assert.assertEquals("test", hit.getIndex());
      Assert.assertEquals("testing", hit.getType());
      Assert.assertEquals("Bob", hit.getId());
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
