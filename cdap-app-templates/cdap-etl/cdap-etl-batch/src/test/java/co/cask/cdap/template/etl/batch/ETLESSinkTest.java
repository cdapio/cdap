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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.batch.sink.ElasticsearchSink;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hsqldb.Server;
import org.hsqldb.server.ServerAcl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.rowset.serial.SerialBlob;

/**
 * <p>
 *  Unit test for {@link ElasticsearchSink} ETL realtime source class.
 * </p>
 */
public class ETLESSinkTest extends BaseETLBatchTest{

  private static final long currentTs = System.currentTimeMillis();
  private static final String clobData = "this is a long string with line separators \n that can be used as \n a clob";

  private static Node node;
  private static Client client;

  private static Schema schema;

  private static final Gson GSON = new Gson();

  private static final Schema BODY_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  private static final Schema EVENT_SCHEMA = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void beforeTest() throws Exception{
    super.beforeTest();
    node = nodeBuilder().local(true).node();
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
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.16");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, "myStream")
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("Elasticsearch",
                                 ImmutableMap.of(Properties.Elasticsearch.HOST, node.settings().get("hostname"),
                                                 Properties.Elasticsearch.PORT, "9200",
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

    QueryBuilder qb = matchQuery("ticker", "AAPL");

    SearchResponse searchResponse = client.prepareSearch().execute().actionGet();
    ElasticsearchAssertions.assertHitCount(searchResponse, 2);
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "AAPL")).execute().actionGet();
    ElasticsearchAssertions.assertHitCount(searchResponse, 1);
    ElasticsearchAssertions.assertFirstHit(searchResponse, ElasticsearchAssertions.hasIndex("test"));
    ElasticsearchAssertions.assertFirstHit(searchResponse, ElasticsearchAssertions.hasType("testing"));
    ElasticsearchAssertions.assertFirstHit(searchResponse, ElasticsearchAssertions.hasId("AAPL"));
    searchResponse = client.prepareSearch().setQuery(matchQuery("ticker", "ABCD")).execute().actionGet();
    ElasticsearchAssertions.assertHitCount(searchResponse, 0);
  }
}

