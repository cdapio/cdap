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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.template.etl.realtime.sink.RealtimeCubeSink;
import co.cask.cdap.template.etl.realtime.sink.RealtimeTableSink;
import co.cask.cdap.template.etl.realtime.sink.StreamSink;
import co.cask.cdap.template.etl.realtime.source.JmsSource;
import co.cask.cdap.template.etl.realtime.source.KafkaSource;
import co.cask.cdap.template.etl.realtime.source.TestSource;
import co.cask.cdap.template.etl.realtime.source.TwitterSource;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.template.etl.transform.ScriptFilterTransform;
import co.cask.cdap.template.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
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
  private static final Id.Namespace NAMESPACE = Constants.DEFAULT_NAMESPACE_ID;
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLRealtime");

  @BeforeClass
  public static void setupTests() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "realtime-sources-1.0.0.jar",
      TestSource.class, JmsSource.class, KafkaSource.class, TwitterSource.class);
    addTemplatePlugins(TEMPLATE_ID, "realtime-sinks-1.0.0.jar",
      RealtimeCubeSink.class, RealtimeTableSink.class, StreamSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar",
      ProjectionTransform.class, ScriptFilterTransform.class, StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLRealtimeTemplate.class,
      PipelineConfigurable.class.getPackage().getName(),
      ETLStage.class.getPackage().getName(),
      RealtimeSource.class.getPackage().getName());
  }

  @Test
  public void testEmptyProperties() throws Exception {
    // Set properties to null to test if ETLTemplate can handle it.
    ETLStage source = new ETLStage("Test", null);
    ETLStage sink = new ETLStage("Stream", ImmutableMap.of(Properties.Stream.NAME, "testS"));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());

    AdapterConfig adapterConfig = new AdapterConfig("null properties", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "testAdap");
    AdapterManager adapterManager = createAdapter(adapterId, adapterConfig);
    Assert.assertNotNull(adapterManager);
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
    // Let the worker run for 3 seconds
    TimeUnit.SECONDS.sleep(3);
    adapterManager.stop();

    // Start again to see if the state is maintained
    adapterManager.start();
    // Let the worker run for 2 seconds
    TimeUnit.SECONDS.sleep(2);
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

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTableSink() throws Exception {
    ETLStage source = new ETLStage("Test", ImmutableMap.of(TestSource.PROPERTY_TYPE, TestSource.TABLE_TYPE));
    ETLStage sink = new ETLStage("Table",
                                 ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1",
                                   Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "binary"));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "testTableSink");

    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    // Let the worker run for 5 seconds
    TimeUnit.SECONDS.sleep(5);
    manager.stop();

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
}
