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
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchApplication} for Stream conversion from stream to avro format for writing to
 * {@link TimePartitionedFileSet}
 */
public class ETLStreamConversionTest extends BaseETLBatchTest {

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
  public void testStreamConversionTPFSParquetSink() throws Exception {
    String sinkType = "TPFSParquet";
    testSink(sinkType);
  }

  @Test
  public void testStreamConversionTPFSAvroSink() throws Exception {
    String sinkType = "TPFSAvro";
    testSink(sinkType);
  }

  private void testSink(String sinkType) throws Exception {
    String filesetName = "converted_stream";
    StreamManager streamManager = getStreamManager("myStream");
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");

    ETLBatchConfig etlConfig = constructETLBatchConfig(filesetName, sinkType);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sconversion");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(4, TimeUnit.MINUTES);

    // get the output fileset, and read the parquet/avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, EVENT_SCHEMA);
    Assert.assertEquals(1, records.size());

  }

  private ETLBatchConfig constructETLBatchConfig(String fileSetName, String sinkType) {
    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, "myStream")
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.DELAY, "0d")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());
    ETLStage sink = new ETLStage(sinkType,
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                 EVENT_SCHEMA.toString(),
                                                 Properties.TimePartitionedFileSetDataset.TPFS_NAME, fileSetName));
    ETLStage transform = new ETLStage("Projection", ImmutableMap.<String, String>of());
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }
}
