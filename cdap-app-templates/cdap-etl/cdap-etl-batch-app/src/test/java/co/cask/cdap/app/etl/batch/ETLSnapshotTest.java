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

package co.cask.cdap.app.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.app.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.template.etl.batch.sink.SnapshotFileBatchSink;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link SnapshotFileBatchSink}.
 */
public class ETLSnapshotTest extends BaseETLBatchTest {
  private static final String STREAM_NAME = "myStream";
  private static final String PATH = "latest";

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

  @Test
  @Category(SlowTests.class)
  public void testParquetSnapshot() throws Exception {
    String streamName = STREAM_NAME + "Parquet";
    StreamManager streamManager = getStreamManager(streamName);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, streamName)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("SnapshotParquet", ImmutableMap.<String, String>builder()
      .put(Properties.SnapshotFileSet.NAME, "testParquet")
      .put(Properties.SnapshotFileSet.PATH_EXTENSION, PATH)
      .put(Properties.SnapshotFileSet.SCHEMA, EVENT_SCHEMA.toString())
      .build());

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "snapshotParquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<FileSet> fileSetManager = getDataset("testParquet");
    FileSet fileSet = fileSetManager.get();

    Location baseLocation = fileSet.getBaseLocation();
    Assert.assertEquals(1, baseLocation.list().size());
    Assert.assertEquals(PATH, baseLocation.list().get(0).getName());
    Assert.assertTrue(!baseLocation.list().get(0).list().isEmpty());

    long time = System.currentTimeMillis();
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(1, baseLocation.list().size());
    Assert.assertEquals(PATH, baseLocation.list().get(0).getName());
    Assert.assertTrue(baseLocation.list().get(0).lastModified() > time);
    Assert.assertTrue(!baseLocation.list().get(0).list().isEmpty());
  }

  @Test
  @Category(SlowTests.class)
  public void testAvroSnapshot() throws Exception {
    String streamName = STREAM_NAME + "Avro";
    StreamManager streamManager = getStreamManager(streamName);
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");
    streamManager.send(ImmutableMap.of("header1", "bar"), "CDAP|13|212.36");

    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, streamName)
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());

    ETLStage sink = new ETLStage("SnapshotAvro", ImmutableMap.<String, String>builder()
      .put(Properties.SnapshotFileSet.NAME, "testAvro")
      .put(Properties.SnapshotFileSet.PATH_EXTENSION, PATH)
      .put(Properties.SnapshotFileSet.SCHEMA, EVENT_SCHEMA.toString())
      .build());

    List<ETLStage> transforms = new ArrayList<>();
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transforms);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "snapshotAvroTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<FileSet> fileSetManager = getDataset("testAvro");
    FileSet fileSet = fileSetManager.get();

    Location baseLocation = fileSet.getBaseLocation();
    Assert.assertEquals(1, baseLocation.list().size());
    Assert.assertEquals(PATH, baseLocation.list().get(0).getName());
    Assert.assertTrue(!baseLocation.list().get(0).list().isEmpty());

    long time = System.currentTimeMillis();
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    Assert.assertEquals(1, baseLocation.list().size());
    Assert.assertEquals(PATH, baseLocation.list().get(0).getName());
    Assert.assertTrue(baseLocation.list().get(0).lastModified() > time);
    Assert.assertTrue(!baseLocation.list().get(0).list().isEmpty());
  }
}
