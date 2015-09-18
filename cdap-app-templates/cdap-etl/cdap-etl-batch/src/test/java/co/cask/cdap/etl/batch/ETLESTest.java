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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.sink.BatchElasticsearchSink;
import co.cask.cdap.etl.batch.source.ElasticsearchSource;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *  Unit test for batch {@link BatchElasticsearchSink} and {@link ElasticsearchSource} classes.
 */
public class ETLESTest extends BaseETLBatchTest {
  private static final Gson GSON = new Gson();
  private static final String STREAM_NAME = "myStream";
  private static final String TABLE_NAME = "outputTable";

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Client client;
  private Node node;
  private int port;


  @Before
  public void beforeTest() throws Exception {
    port = Networks.getRandomPort();
    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
      .put("path.data", tmpFolder.newFolder("data"))
      .put("http.port", port);
    node = nodeBuilder().settings(elasticsearchSettings.build()).client(false).node();
    client = node.client();
  }

  @After
  public void afterTest() {
    try {
      DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest("test")).actionGet();
      Assert.assertTrue(delete.isAcknowledged());
    } finally {
      node.close();
    }
  }

  @Test
  @Category(SlowTests.class)
  public void testES() throws Exception {
    testESSink();
    testESSource();
  }

  private void testESSink() throws Exception {
    StreamManager streamManager = getStreamManager(STREAM_NAME);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, STREAM_NAME)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("Elasticsearch",
                                 ImmutableMap.of(Properties.Elasticsearch.HOST,
                                   InetAddress.getLocalHost().getHostName() + ":" + port,
                                   Properties.Elasticsearch.INDEX_NAME, "test",
                                   Properties.Elasticsearch.TYPE_NAME, "testing",
                                   Properties.Elasticsearch.ID_FIELD, "ticker"
                                 ));
    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "esSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    SearchResponse searchResponse = client.prepareSearch("test").execute().actionGet();
    Assert.assertEquals(2, searchResponse.getHits().getTotalHits());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
    Assert.assertEquals(1, searchResponse.getHits().getTotalHits());
    Assert.assertEquals("test", searchResponse.getHits().getAt(0).getIndex());
    Assert.assertEquals("testing", searchResponse.getHits().getAt(0).getType());
    Assert.assertEquals("AAPL", searchResponse.getHits().getAt(0).getId());
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
    Assert.assertEquals(0, searchResponse.getHits().getTotalHits());

    DeleteResponse response = client.prepareDelete("test", "testing", "CDAP").execute().actionGet();
    Assert.assertTrue(response.isFound());
  }

  @SuppressWarnings("ConstantConditions")
  private void testESSource() throws Exception {
    ETLStage source = new ETLStage("Elasticsearch",
                                   ImmutableMap.of(Properties.Elasticsearch.HOST,
                                                   InetAddress.getLocalHost().getHostName() + ":" + port,
                                                   Properties.Elasticsearch.INDEX_NAME, "test",
                                                   Properties.Elasticsearch.TYPE_NAME, "testing",
                                                   Properties.Elasticsearch.QUERY, "?q=*",
                                                   Properties.Elasticsearch.SCHEMA, BODY_SCHEMA.toString()));
    ETLStage sink = new ETLStage("Table",
                                 ImmutableMap.of("name", TABLE_NAME,
                                                 Properties.Table.PROPERTY_SCHEMA, BODY_SCHEMA.toString(),
                                                 Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "ticker"));

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "esSourceTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(TABLE_NAME);
    Table outputTable = outputManager.get();

    // Scanner to verify number of rows
    Scanner scanner = outputTable.scan(null, null);
    Row row1 = scanner.next();
    Assert.assertNotNull(row1);
    Assert.assertNull(scanner.next());
    scanner.close();
    // Verify data
    Assert.assertEquals(10, (int) row1.getInt("num"));
    Assert.assertEquals(500.32, row1.getDouble("price"), 0.000001);
    Assert.assertNull(row1.get("NOT_IMPORTED"));
  }
}
