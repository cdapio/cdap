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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.common.MockAdapterConfigurer;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.templates.etl.realtime.sinks.RealtimeTableSink;
import co.cask.cdap.templates.etl.realtime.sinks.StreamSink;
import co.cask.cdap.templates.etl.realtime.sources.TestSource;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkerManager;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLRealtimeTemplate}.
 */
public class ETLWorkerTest extends TestBase {
  private static ApplicationManager templateManager;

  @BeforeClass
  public static void setupTests() {
    templateManager = deployApplication(ETLRealtimeTemplate.class);
  }

  // TODO: Remove ignore once end-to-end testing is figured out with plugins
  @Ignore
  @Test
  @Category(SlowTests.class)
  public void testStreamSink() throws Exception {
    StreamManager streamManager = getStreamManager(Constants.DEFAULT_NAMESPACE_ID, "testStream");
    streamManager.createStream();

    ApplicationTemplate<ETLRealtimeConfig> appTemplate = new ETLRealtimeTemplate();

    long startTime = System.currentTimeMillis();
    ETLStage source = new ETLStage(TestSource.class.getSimpleName(),
                                   ImmutableMap.of(TestSource.PROPERTY_TYPE, TestSource.STREAM_TYPE));
    ETLStage sink = new ETLStage(StreamSink.class.getSimpleName(),
                                 ImmutableMap.of(Properties.Stream.NAME, "testStream"));
    ETLRealtimeConfig adapterConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());
    MockAdapterConfigurer adapterConfigurer = new MockAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);
    Map<String, String> workerArgs = Maps.newHashMap(adapterConfigurer.getArguments());
    WorkerManager workerManager = templateManager.startWorker(ETLWorker.class.getSimpleName(), workerArgs);
    // Let the worker run for 5 seconds
    TimeUnit.SECONDS.sleep(5);
    workerManager.stop();
    templateManager.stopAll();

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
    ApplicationTemplate<ETLRealtimeConfig> appTemplate = new ETLRealtimeTemplate();
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
    Assert.assertNotNull(row.getLong("time"));
  }

  private void addDatasetInstances(MockAdapterConfigurer configurer) throws Exception {
    for (Map.Entry<String, KeyValue<String, DatasetProperties>> entry :
      configurer.getDatasetInstances().entrySet()) {
      String typeName = entry.getValue().getKey();
      DatasetProperties properties = entry.getValue().getValue();
      String instanceName = entry.getKey();
      addDatasetInstance(typeName, instanceName, properties);
    }
  }
}
