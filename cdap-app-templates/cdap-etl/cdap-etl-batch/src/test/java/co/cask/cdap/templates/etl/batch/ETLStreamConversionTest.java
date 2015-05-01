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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.BatchCubeSink;
import co.cask.cdap.templates.etl.batch.sinks.DBSink;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.common.ETLStage;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.ProjectionTransform;
import co.cask.cdap.templates.etl.transforms.ScriptFilterTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchTemplate} for Stream conversion from stream to avro format for writing to
 * {@link TimePartitionedFileSet}
 */
// TODO: re-enable after mapred classloading fix
@Ignore
public class ETLStreamConversionTest extends TestBase {
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

  private static final Id.Namespace NAMESPACE = Id.Namespace.from("default");
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("etlBatch");

  @BeforeClass
  public static void setupTemplate() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "batch-sources-1.0.0.jar",
      DBSource.class, KVTableSource.class, StreamBatchSource.class, TableSource.class);
    addTemplatePlugins(TEMPLATE_ID, "batch-sinks-1.0.0.jar",
      BatchCubeSink.class, DBSink.class, KVTableSink.class,
      TableSink.class, TimePartitionedFileSetDatasetAvroSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar", IdentityTransform.class,
      ProjectionTransform.class, ScriptFilterTransform.class, StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLBatchTemplate.class,
      EndPointStage.class.getPackage().getName(),
      ETLStage.class.getPackage().getName(),
      BatchSource.class.getPackage().getName());
  }

  @Test
  public void testStreamConversion() throws Exception {
    String filesetName = "converted_stream";
    StreamManager streamManager = getStreamManager("myStream");
    streamManager.createStream();
    streamManager.send(ImmutableMap.of("header1", "bar"), "AAPL|10|500.32");

    ETLBatchConfig etlConfig = constructETLBatchConfig(filesetName);
    AdapterConfig adapterConfig = new AdapterConfig("description", "etlBatch", GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "sconversion");
    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    manager.start();
    manager.waitForOneRunToFinish(4, TimeUnit.MINUTES);
    manager.stop();

    // get the output fileset, and read the avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, EVENT_SCHEMA);
    Assert.assertEquals(1, records.size());
  }

  private ETLBatchConfig constructETLBatchConfig(String fileSetName) {
    ETLStage source = new ETLStage("Stream", ImmutableMap.<String, String>builder()
      .put(Properties.Stream.NAME, "myStream")
      .put(Properties.Stream.DURATION, "10m")
      .put(Properties.Stream.FORMAT, Formats.CSV)
      .put(Properties.Stream.SCHEMA, BODY_SCHEMA.toString())
      .put("format.setting.delimiter", "|")
      .build());
    ETLStage sink = new ETLStage("TPFSAvro",
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                 EVENT_SCHEMA.toString(),
                                                 Properties.TimePartitionedFileSetDataset.TPFS_NAME, fileSetName));
    return new ETLBatchConfig("* * * * *", source, sink, Lists.<ETLStage>newArrayList());
  }

  private List<GenericRecord> readOutput(TimePartitionedFileSet fileSet, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(avroSchema);
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
              new DataFileStream<GenericRecord>(file.getInputStream(), datumReader);
            while (fileStream.hasNext()) {
              records.add(fileStream.next());
            }
          }
        }
      }
    }
    return records;
  }
}
