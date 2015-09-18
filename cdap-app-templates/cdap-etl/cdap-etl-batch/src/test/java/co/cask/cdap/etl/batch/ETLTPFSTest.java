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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ETLTPFSTest extends BaseETLBatchTest {

  @Test
  public void testAvroSourceConversionToAvroSink() throws Exception {

    Schema eventSchema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)));

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(eventSchema.toString());

    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .build();

    String filesetName = "tpfs";
    addDatasetInstance(TimePartitionedFileSet.class.getName(), filesetName, FileSetProperties.builder()
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setInputProperty("schema", avroSchema.toString())
      .setOutputProperty("schema", avroSchema.toString())
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (avroSchema.toString()))
      .build());
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet tpfs = fileSetManager.get();

    TransactionManager txService = getTxService();
    Transaction tx1 = txService.startShort(100);
    TransactionAware txTpfs = (TransactionAware) tpfs;
    txTpfs.startTx(tx1);

    long timeInMillis = System.currentTimeMillis();
    fileSetManager.get().addPartition(timeInMillis, "directory", ImmutableMap.of("key1", "value1"));
    Location location = fileSetManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.avro");
    FSDataOutputStream outputStream = new FSDataOutputStream(location.getOutputStream(), null);
    DataFileWriter dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(avroSchema));
    dataFileWriter.create(avroSchema, outputStream);
    dataFileWriter.append(record);
    dataFileWriter.flush();

    txTpfs.commitTx();
    txService.canCommit(tx1, txTpfs.getTxChanges());
    txService.commit(tx1);
    txTpfs.postTxCommit();

    String newFilesetName = filesetName + "_op";
    ETLBatchConfig etlBatchConfig = constructTPFSETLConfig(filesetName, newFilesetName, eventSchema);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlBatchConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sconversion1");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("runtime", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> newFileSetManager = getDataset(newFilesetName);
    TimePartitionedFileSet newFileSet = newFileSetManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, eventSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, newRecords.get(0).get("int"));
  }

  private ETLBatchConfig constructTPFSETLConfig(String filesetName, String newFilesetName, Schema eventSchema) {
    ETLStage source = new ETLStage("TPFSAvro",
                                   ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                   eventSchema.toString(),
                                                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, filesetName,
                                                   Properties.TimePartitionedFileSetDataset.DELAY, "0d",
                                                   Properties.TimePartitionedFileSetDataset.DURATION, "2m"));
    ETLStage sink = new ETLStage("TPFSAvro",
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                 eventSchema.toString(),
                                                 Properties.TimePartitionedFileSetDataset.TPFS_NAME,
                                                 newFilesetName));

    ETLStage transform = new ETLStage("Projection", ImmutableMap.<String, String>of());
    return new ETLBatchConfig("* * * * *", source, sink, Lists.newArrayList(transform));
  }
}
