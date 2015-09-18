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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.source.FileBatchSource;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.test.sink.MetaKVTableSink;
import co.cask.cdap.etl.test.source.MetaKVTableSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTest extends BaseETLBatchTest {

  @Test
  public void testInvalidTransformConfigFailsToDeploy() {
    ETLStage source = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"));
    ETLStage sink = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"));
    ETLStage transform = new ETLStage("Script", ImmutableMap.of("script", "return x;"));
    List<ETLStage> transformList = Lists.newArrayList(transform);
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testKVToKV() throws Exception {
    // kv table to kv table pipeline
    ETLStage source = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"));
    ETLStage sink = new ETLStage("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"));
    ETLStage transform = new ETLStage("Projection", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(transform);
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 10000; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> table2 = getDataset("table2");
    KeyValueTable outputTable = table2.get();
    for (int i = 0; i < 10000; i++) {
      Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
    }
  }

  @Test
  public void testKVToKVMeta() throws Exception {
    ETLStage source = new ETLStage("MetaKVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable1"));
    ETLStage sink = new ETLStage("MetaKVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable2"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKVMeta");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> sourceMetaTable = getDataset(MetaKVTableSource.META_TABLE);
    KeyValueTable sourceTable = sourceMetaTable.get();
    Assert.assertEquals(MetaKVTableSource.PREPARE_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.PREPARE_RUN_KEY)));
    Assert.assertEquals(MetaKVTableSource.FINISH_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.FINISH_RUN_KEY)));

    DataSetManager<KeyValueTable> sinkMetaTable = getDataset(MetaKVTableSink.META_TABLE);
    KeyValueTable sinkTable = sinkMetaTable.get();
    Assert.assertEquals(MetaKVTableSink.PREPARE_RUN_KEY,
                        Bytes.toString(sinkTable.read(MetaKVTableSink.PREPARE_RUN_KEY)));
    Assert.assertEquals(MetaKVTableSink.FINISH_RUN_KEY,
                        Bytes.toString(sinkTable.read(MetaKVTableSink.FINISH_RUN_KEY)));
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTableToTableWithValidations() throws Exception {

    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );

    ETLStage source = new ETLStage("Table",
      ImmutableMap.of(
        Properties.BatchReadableWritable.NAME, "inputTable",
        Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
        Properties.Table.PROPERTY_SCHEMA, schema.toString()));

    String validationScript = "function isValid(input) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.user, 6)) " +
      "{ errCode = 10; errMsg = 'user name greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    ETLStage transform = new ETLStage("Validator",
                                      ImmutableMap.of("validators", "core",
                                                      "validationScript", validationScript),
                                      "keyErrors");
    List<ETLStage> transformList = new ArrayList<>();
    transformList.add(transform);

    ETLStage sink = new ETLStage("Table",
      ImmutableMap.of(
        Properties.BatchReadableWritable.NAME, "outputTable",
        Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey"));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TableToTable");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("inputTable");
    Table inputTable = inputManager.get();

    // valid record, user name "samuel" is 6 chars long
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();

    // valid record, user name "jackson" is > 6 characters
    put = new Put(Bytes.toBytes("row2"));
    put.add("user", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);
    inputManager.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("user"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertEquals(0, row.getColumns().size());

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("keyErrors");
    TimePartitionedFileSet fileSet = fileSetManager.get();
    List<GenericRecord> records = readOutput(fileSet, ETLMapReduce.ERROR_SCHEMA);
    Assert.assertEquals(1, records.size());
    fileSet.close();

  }

  @Test
  public void testS3toTPFS() throws Exception {
    String testPath = "s3n://test/2015-06-17-00-00-00.txt";
    String testData = "Sample data for testing.";

    S3NInMemoryFileSystem fs = new S3NInMemoryFileSystem();
    Configuration conf = new Configuration();
    conf.set("fs.s3n.impl", S3NInMemoryFileSystem.class.getName());
    fs.initialize(URI.create("s3n://test/"), conf);
    fs.createNewFile(new Path(testPath));

    FSDataOutputStream writeData = fs.create(new Path(testPath));
    writeData.write(testData.getBytes());
    writeData.flush();
    writeData.close();

    Method method = FileSystem.class.getDeclaredMethod("addFileSystemForTesting",
                                                       new Class[]{URI.class, Configuration.class, FileSystem.class});
    method.setAccessible(true);
    method.invoke(FileSystem.class, URI.create("s3n://test/"), conf, fs);
    ETLStage source = new ETLStage("S3", ImmutableMap.<String, String>builder()
      .put(Properties.S3.ACCESS_KEY, "key")
      .put(Properties.S3.ACCESS_ID, "ID")
      .put(Properties.S3.PATH, testPath)
      .build());

    ETLStage sink = new ETLStage("TPFSAvro",
                                 ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                   FileBatchSource.DEFAULT_SCHEMA.toString(),
                                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, "TPFSsink"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", source, sink, Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "S3ToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("TPFSsink");
    TimePartitionedFileSet fileSet = fileSetManager.get();
    List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals(testData, records.get(0).get("body").toString());
    fileSet.close();
  }

  @Test
  public void testFiletoMultipleTPFS() throws Exception {
    String filePath = "file:///tmp/test/text.txt";
    String testData = "String for testing purposes.";

    Path textFile = new Path(filePath);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream writeData = fs.create(textFile);
    writeData.write(testData.getBytes());
    writeData.flush();
    writeData.close();

    ETLStage source = new ETLStage("File", ImmutableMap.<String, String>builder()
      .put(Properties.File.FILESYSTEM, "Text")
      .put(Properties.File.PATH, filePath)
      .build());

    ETLStage sink1 = new ETLStage("TPFSAvro",
                                  ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink1"));
    ETLStage sink2 = new ETLStage("TPFSParquet",
                                  ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink2"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  source,
                                                  Lists.newArrayList(sink1, sink2),
                                                  Lists.<ETLStage>newArrayList(),
                                                  new Resources(),
                                                  Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    for (String sinkName : new String[] { "fileSink1", "fileSink2" }) {
      DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(sinkName);
      TimePartitionedFileSet fileSet = fileSetManager.get();
      List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(testData, records.get(0).get("body").toString());
      fileSet.close();
    }
  }

}
