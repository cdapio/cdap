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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.batch.sink.ElasticsearchSink;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * <p>
 *  Unit test for {@link ElasticsearchSink} ETL batch sink class.
 * </p>
 */
public class ETLESSinkTest extends BaseETLBatchTest {
  private Client client;
  private Node node;

  private static final Gson GSON = new Gson();

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void beforeTest() throws Exception {
    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
      .put("path.data", tmpFolder.newFolder("data"));
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
    StreamManager streamManager = getStreamManager("myStream");
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, "myStream")
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    try {
      ETLStage sink = new ETLStage("Elasticsearch",
                                   ImmutableMap.of(Properties.Elasticsearch.HOST, "localhost:9200",
                                                   Properties.Elasticsearch.INDEX_NAME, "test",
                                                   Properties.Elasticsearch.TYPE_NAME, "testing",
                                                   Properties.Elasticsearch.ID_FIELD, "ticker"
                                   ));
      List<ETLStage> transforms = Lists.newArrayList();
      ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);
      Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "esSinkTest");
      AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
      AdapterManager manager = createAdapter(adapterId, adapterConfig);

      manager.start();
      manager.waitForOneRunToFinish(5, TimeUnit.MINUTES);
      manager.stop();
      SearchResponse searchResponse = client.prepareSearch("test").execute().actionGet();
      Assert.assertEquals(searchResponse.getHits().getTotalHits(), 2);
      searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
      Assert.assertEquals(searchResponse.getHits().getTotalHits(), 1);
      Assert.assertEquals(searchResponse.getHits().getAt(0).getIndex(), "test");
      Assert.assertEquals(searchResponse.getHits().getAt(0).getType(), "testing");
      Assert.assertEquals(searchResponse.getHits().getAt(0).getId(), "AAPL");
      searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
      Assert.assertEquals(searchResponse.getHits().getTotalHits(), 0);

      DeleteResponse response = client.prepareDelete("test", "testing", "CDAP").execute().actionGet();
      Assert.assertTrue(response.isFound());
    } finally {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("test")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    }
  }
}

