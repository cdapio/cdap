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
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchTemplate} for Stream conversion from stream to avro format for writing to
 * {@link TimePartitionedFileSet}
 */
public class ETLStreamConversionTest extends BaseETLBatchTest {
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

  // The new event scheme is being used over the old schema because Maps when parsed take ordering of keys into account.
  // Once CDAP-2813 is resolved, we can switch to using EVENT_SCHEMA
  private static final Schema EVENT_SCHEMA_1 = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
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

  @Test
  public void testAvroSourceConversionToAvroSink() throws Exception {
    String filesetName = "converted_stream";
    StreamManager streamManager = getStreamManager("myStream");
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");

    ETLBatchConfig etlConfig = constructETLBatchConfig(filesetName, "TPFSAvro");
    AdapterConfig adapterConfig = new AdapterConfig("description", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "sconversion");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    manager.stop();

    // get the output fileset, and read the parquet/avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, EVENT_SCHEMA_1);
    Assert.assertEquals(1, records.size());

    String newFilesetName = filesetName + "_op";
    ETLBatchConfig etlBatchConfig = constructTPFSETLConfig(filesetName, newFilesetName);

    AdapterConfig newAdapterConfig = new AdapterConfig("description", TEMPLATE_ID.getId(),
                                                       GSON.toJsonTree(etlBatchConfig));
    Id.Adapter newAdapterId = Id.Adapter.from(NAMESPACE, "sconversion1");
    AdapterManager tpfsAdapterManager = createAdapter(newAdapterId, newAdapterConfig);

    tpfsAdapterManager.start();
    tpfsAdapterManager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    tpfsAdapterManager.stop();

    DataSetManager<TimePartitionedFileSet> newFileSetManager = getDataset(newFilesetName);
    TimePartitionedFileSet newFileSet = newFileSetManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, EVENT_SCHEMA_1);
    Assert.assertEquals(1, newRecords.size());

  }

  private void testSink(String sinkType) throws Exception {
    String filesetName = "converted_stream";
    StreamManager streamManager = getStreamManager("myStream");
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");

    ETLBatchConfig etlConfig = constructETLBatchConfig(filesetName, sinkType);
    AdapterConfig adapterConfig = new AdapterConfig("description", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "sconversion");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    manager.stop();

    // get the output fileset, and read the parquet/avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, EVENT_SCHEMA_1);
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

  private ETLBatchConfig constructTPFSETLConfig(String filesetName, String newFilesetName) {
    ETLStage source = new ETLStage("TPFSAvro",
                                   ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                   EVENT_SCHEMA_1.toString(),
                                                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, filesetName,
                                                   Properties.TimePartitionedFileSetDataset.DELAY, "0d",
                                                   Properties.TimePartitionedFileSetDataset.DURATION, "10m"));
    ETLStage sink = new ETLStage("TPFSAvro",
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                 EVENT_SCHEMA_1.toString(),
                                                 Properties.TimePartitionedFileSetDataset.TPFS_NAME,
                                                 newFilesetName));

    // The header field has been dropped because it contains maps. This map when parsed by the Avro Source which follows
    // leads to an exception because the "values" field precedes the "keys" and the parser thinks that "keys" is not
    // present. Once CDAP-2813 is resolved, we can stop dropping headers.
    ETLStage transform = new ETLStage("Projection", ImmutableMap.of("drop", "headers"));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }

  protected List<GenericRecord> readOutput(TimePartitionedFileSet fileSet, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records = com.google.common.collect.Lists.newArrayList();
    for (Location dayLoc : fileSet.getEmbeddedFileSet().getBaseLocation().list()) {
      // this level should be the day (ex: 2015-01-19)
      for (Location timeLoc : dayLoc.list()) {
        // this level should be the time (ex: 21-23.1234567890000)
        for (Location file : timeLoc.list()) {
          // this level should be the actual mapred output
          String locName = file.getName();

          if (locName.endsWith(".avro")) {
            DataFileStream<GenericRecord> fileStream =
              new DataFileStream<>(file.getInputStream(), datumReader);
            while (fileStream.hasNext()) {
              records.add(fileStream.next());
            }
          }

          if (locName.endsWith(".parquet")) {
            Path parquetFile = new Path(file.toString());
            AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(parquetFile);
            GenericRecord result = reader.read();
            while (result != null) {
              records.add(result);
              result = reader.read();
            }
          }
        }
      }
    }
    return records;
  }
}
