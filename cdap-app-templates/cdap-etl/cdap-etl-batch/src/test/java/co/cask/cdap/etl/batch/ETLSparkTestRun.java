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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ETLSparkTestRun extends ETLBatchTestBase {

  @Test
  public void test() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG))
    );

    ETLStage source = new ETLStage("source",
                                   new Plugin(
                                     "TPFSParquet",
                                     ImmutableMap.of(
                                       Properties.TimePartitionedFileSetDataset.SCHEMA, recordSchema.toString(),
                                       Properties.TimePartitionedFileSetDataset.TPFS_NAME, "inputParquetSpark",
                                       Properties.TimePartitionedFileSetDataset.DELAY, "0d",
                                       Properties.TimePartitionedFileSetDataset.DURATION, "1h")));
    ETLStage transform = new ETLStage("transform",
                                      new Plugin(
                                        "Script",
                                        ImmutableMap.of("script",
                                                        "function transform(input, context) { " +
                                                          "return {'i': input.i * 2, 'l': input.l * 2}; " +
                                                          "}")));
    ETLStage sink1 = new ETLStage("sink1",
                                  new Plugin(
                                    "TPFSParquet",
                                    ImmutableMap.of(
                                      Properties.TimePartitionedFileSetDataset.SCHEMA, recordSchema.toString(),
                                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "transformedOutput")));
    ETLStage sink2 = new ETLStage("sink2",
                                  new Plugin(
                                    "TPFSParquet",
                                    ImmutableMap.of(
                                      Properties.TimePartitionedFileSetDataset.SCHEMA, recordSchema.toString(),
                                      Properties.TimePartitionedFileSetDataset.TPFS_NAME, "identityOutput")));
    List<ETLStage> sinks = ImmutableList.of(sink1, sink2);
    // pipeline will transform the input before writing to sink1. It will also write directly to sink2 without transform
    List<Connection> connections = ImmutableList.of(
      new Connection("source", "transform"),
      new Connection("transform", "sink1"),
      new Connection("source", "sink2")
    );
    ETLBatchConfig etlConfig = new ETLBatchConfig(ETLBatchConfig.Engine.SPARK, "* * * * *",
                                                  source, sinks, ImmutableList.of(transform),
                                                  connections, new Resources(), ImmutableList.<ETLStage>of());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "parquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", 1)
      .set("l", 2L)
      .build();
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("inputParquetSpark");

    long timeInMillis = System.currentTimeMillis();
    inputManager.get().addPartition(timeInMillis, "directory");
    inputManager.flush();
    Location location = inputManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.parquet");
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(location.toURI().toString()), avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("runtime", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    // check output of the first output, which was transformed before it was written
    DataSetManager<TimePartitionedFileSet> outputManager = getDataset("transformedOutput");
    TimePartitionedFileSet transformedFileSet = outputManager.get();

    List<GenericRecord> transformedRecords = readOutput(transformedFileSet, recordSchema);
    Assert.assertEquals(1, transformedRecords.size());
    Assert.assertEquals(2, transformedRecords.get(0).get("i"));
    Assert.assertEquals(4L, transformedRecords.get(0).get("l"));

    // check output of the second output, which was written directly without any transform
    outputManager = getDataset("identityOutput");
    TimePartitionedFileSet identityFileSet = outputManager.get();
    List<GenericRecord> identityRecords = readOutput(identityFileSet, recordSchema);
    Assert.assertEquals(1, identityRecords.size());
    Assert.assertEquals(1, identityRecords.get(0).get("i"));
    Assert.assertEquals(2L, identityRecords.get(0).get("l"));
  }
}
