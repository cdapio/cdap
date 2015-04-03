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
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.templates.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.common.config.ETLStage;
import co.cask.cdap.templates.etl.transforms.StreamToStructuredRecordTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToAvroTransform;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link ETLBatchTemplate} for Stream conversion from stream to avro format for writing to
 * {@link TimePartitionedFileSet}
 */
public class ETLStreamConversionTest extends TestBase {

  private static final Gson GSON = new Gson();

  Schema bodySchema = Schema.recordOf(
    "event",
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  Schema eventSchema = Schema.recordOf(
    "streamEvent",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("headers", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),
    Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("num", Schema.of(Schema.Type.INT)),
    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)));

  @Test
  public void testConfig() throws Exception {
    String filesetName = "converted_stream";

    addDatasetInstance("timePartitionedFileSet", filesetName, FileSetProperties.builder()
      .setBasePath(filesetName)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", eventSchema.toString())
      .build());

    ApplicationManager batchManager = deployApplication(ETLBatchTemplate.class);
    StreamWriter streamWriter = batchManager.getStreamWriter("myStream");
    streamWriter.createStream();
    streamWriter.send(ImmutableMap.of("header1", "bar"), "AAPL,10,500.32");

    ApplicationTemplate<ETLBatchConfig> appTemplate = new ETLBatchTemplate();
    ETLBatchConfig adapterConfig = constructETLBatchConfig(filesetName);
    DefaultAdapterConfigurer adapterConfigurer = new DefaultAdapterConfigurer();
    appTemplate.configureAdapter("myAdapter", adapterConfig, adapterConfigurer);

    Map<String, String> mapReduceArgs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : adapterConfigurer.getArguments().entrySet()) {
      mapReduceArgs.put(entry.getKey(), entry.getValue());
    }
    mapReduceArgs.put("config", GSON.toJson(adapterConfig));
    MapReduceManager mrManager = batchManager.startMapReduce("ETLMapReduce", mapReduceArgs);
    mrManager.waitForFinish(5, TimeUnit.MINUTES);
    batchManager.stopAll();
    // get the output fileset, and read the avro files it output.
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet fileSet = fileSetManager.get();

    List<GenericRecord> records = readOutput(fileSet, eventSchema);

    Assert.assertEquals(1, records.size());
  }

  private ETLBatchConfig constructETLBatchConfig(String fileSetName) {
    ETLStage source = new ETLStage(StreamBatchSource.class.getName(),
                                   ImmutableMap.of("streamName", "myStream", "frequency", "30"));
    ETLStage transform1 = new ETLStage(StreamToStructuredRecordTransform.class.getName(),
                                       ImmutableMap.of("schemaType", Formats.CSV, "schema", bodySchema.toString()));
    ETLStage transform2 = new ETLStage(StructuredRecordToAvroTransform.class.getName(), ImmutableMap.<String, String>of());
    ETLStage sink = new ETLStage(TimePartitionedFileSetDatasetAvroSink.class.getName(),
                                 ImmutableMap.of("schema", bodySchema.toString(), "name", fileSetName));
    List<ETLStage> transformList = Lists.newArrayList();
    transformList.add(transform1);
    transformList.add(transform2);
    return new ETLBatchConfig("", source, sink, transformList);
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
