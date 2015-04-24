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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.templates.etl.realtime.sinks.StreamSink;
import co.cask.cdap.templates.etl.realtime.sources.TestSource;
import co.cask.cdap.templates.etl.realtime.sources.TwitterSource;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLRealtimeTemplate}.
 */
public class ETLWorkerTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final Id.Namespace NAMESPACE = Id.Namespace.from("default");
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("etlRealtime");

  @BeforeClass
  public static void setupTests() throws IOException {
    addTemplatePlugin(TEMPLATE_ID, TwitterSource.class, "twitter-source-1.0.0.jar");
    addTemplatePlugin(TEMPLATE_ID, StreamSink.class, "stream-sink-1.0.0.jar");
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLRealtimeTemplate.class,
      EndPointStage.class.getPackage().getName(),
      ETLStage.class.getPackage().getName(),
      RealtimeSource.class.getPackage().getName());
  }

  @Test
  @Category(SlowTests.class)
  public void testStreamSink() throws Exception {
    long startTime = System.currentTimeMillis();
    ETLStage source = new ETLStage("Test", ImmutableMap.of(TestSource.PROPERTY_TYPE, TestSource.STREAM_TYPE));
    ETLStage sink = new ETLStage("Stream", ImmutableMap.of(Properties.Stream.NAME, "testStream"));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());

    AdapterConfig adapterConfig = new AdapterConfig("test adapter", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "testToStream");
    AdapterManager adapterManager = createAdapter(adapterId, adapterConfig);

    adapterManager.start();
    // Let the worker run for 5 seconds
    TimeUnit.SECONDS.sleep(5);
    adapterManager.stop();

    StreamManager streamManager = getStreamManager(NAMESPACE, "testStream");
    List<StreamEvent> streamEvents = streamManager.getEvents(startTime, System.currentTimeMillis(), Integer.MAX_VALUE);
    // verify that some events were sent to the stream
    Assert.assertTrue(streamEvents.size() > 0);
    // since we sent all identical events, verify the contents of just one of them
    Random random = new Random();
    StreamEvent event = streamEvents.get(random.nextInt(streamEvents.size()));
    ByteBuffer body = event.getBody();
    Map<String, String> headers = event.getHeaders();
    if (headers != null && !headers.isEmpty()) {
      Assert.assertEquals("v1", headers.get("h1"));
    }
    Assert.assertEquals("Hello", Bytes.toString(body, Charsets.UTF_8));
  }

  // TODO: Remove ignore once end-to-end testing is figured out with plugins
  @Ignore
  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTableSink() throws Exception {
    /*ApplicationTemplate<ETLRealtimeConfig> appTemplate = new ETLRealtimeTemplate();
    ETLStage source = new ETLStage(TestSource.class.getSimpleName(),
                                   ImmutableMap.of(TestSource.PROPERTY_TYPE, TestSource.TABLE_TYPE));
    ETLStage sink = new ETLStage(RealtimeTableSink.class.getSimpleName(),
                                 ImmutableMap.of("name", "table1",
                                                 Table.PROPERTY_SCHEMA_ROW_FIELD, "binary"));
    ETLRealtimeConfig adapterConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);
    addDatasetInstances(adapterConfigurer);
    Map<String, String> workerArgs = Maps.newHashMap(adapterConfigurer.getArguments());
    WorkerManager workerManager = templateManager.startWorker(ETLWorker.class.getSimpleName(), workerArgs);
    // Let the worker run for 5 seconds
    TimeUnit.SECONDS.sleep(5);
    workerManager.stop();
    templateManager.stopAll();

    // verify
    DataSetManager<Table> tableManager = getDataset("table1");
    Table table = tableManager.get();
    // verify that atleast 1 record got written to the Table
    Row row = table.get("Bob".getBytes(Charsets.UTF_8));

    Assert.assertNotNull("Atleast 1 row should have been exported to the Table sink.", row);
    Assert.assertEquals(1, (int) row.getInt("id"));
    Assert.assertEquals("Bob", row.getString("name"));
    Assert.assertEquals(3.4, row.getDouble("score"), 0.000001);
    Assert.assertEquals(false, row.getBoolean("graduated"));
    Assert.assertNotNull(row.getLong("time"));*/
  }
}
