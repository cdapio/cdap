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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.partitioned.TimePartitionedFileSetDataset;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.DateFormat;
import java.util.Collections;

/**
 * This tests that time partitioned file sets and their partitions are correctly registered
 * in the Hive meta store when created, and also that they are removed from Hive when deleted.
 * This does not test querying through Hive (it will be covered by an integration test).
 */
@Category(SlowTests.class)
public class HiveExploreServiceFileSetTest extends BaseHiveExploreServiceTest {

  private static final Schema SCHEMA = Schema.recordOf("kv",
                                                       Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("value", Schema.of(Schema.Type.STRING)));
  private static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create(), false);
  }

  @After
  public void deleteAll() throws Exception {
    datasetFramework.deleteAllInstances();
  }

  @Test
  public void testCreateAddDrop() throws Exception {

    final String datasetName = "files";

    @SuppressWarnings("UnnecessaryLocalVariable")
    final String tableName = datasetName; // in this test context, the hive table name is the same as the dataset name

    // create a time partitioned file set
    datasetFramework.addInstance("fileSet", datasetName, FileSetProperties.builder()
      // properties for file set
      .setBasePath("/myPath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand("show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    FileSet fileSet = datasetFramework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(fileSet);

    // add a file
    AvroHelper.generateAvroFile(fileSet.getLocation("file1").getOutputStream(), "a", 0, 3);

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT * FROM " + tableName, true,
               Lists.newArrayList(
                 new ColumnDesc("files.key", "STRING", 1, null),
                 new ColumnDesc("files.value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("a0", "#0")),
                 new QueryResult(Lists.<Object>newArrayList("a1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("a2", "#2"))));

    // add another file
    AvroHelper.generateAvroFile(fileSet.getLocation("file2").getOutputStream(), "b", 3, 5);

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT count(*) AS count FROM " + tableName, true,
               Lists.newArrayList(new ColumnDesc("count", "BIGINT", 1, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(5L))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetName);

    // verify the Hive table is gone
    runCommand("show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }


  @Test
  public void testPartitionedFileSet() throws Exception {

    final String datasetName = "parted";

    @SuppressWarnings("UnnecessaryLocalVariable")
    final String tableName = datasetName; // in this test context, the hive table name is the same as the dataset name

    // create a time partitioned file set
    datasetFramework.addInstance("partitionedFileSet", datasetName, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder()
                         .addStringField("str")
                         .addIntField("num")
                         .build())
        // properties for file set
      .setBasePath("/parted")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand("show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add some partitions. Beware that Hive expects a partition to be a directory, so we create dirs with one file
    Location locationX1 = fileSet.getLocation("fileX1/nn");
    Location locationY1 = fileSet.getLocation("fileY1/nn");
    Location locationX2 = fileSet.getLocation("fileX2/nn");
    Location locationY2 = fileSet.getLocation("fileY2/nn");

    AvroHelper.generateAvroFile(locationX1.getOutputStream(), "x", 1, 2);
    AvroHelper.generateAvroFile(locationY1.getOutputStream(), "y", 1, 2);
    AvroHelper.generateAvroFile(locationX2.getOutputStream(), "x", 2, 3);
    AvroHelper.generateAvroFile(locationY2.getOutputStream(), "y", 2, 3);

    PartitionKey keyX1 = PartitionKey.builder().addStringField("str", "x").addIntField("num", 1).build();
    PartitionKey keyY1 = PartitionKey.builder().addStringField("str", "y").addIntField("num", 1).build();
    PartitionKey keyX2 = PartitionKey.builder().addStringField("str", "x").addIntField("num", 2).build();
    PartitionKey keyY2 = PartitionKey.builder().addStringField("str", "y").addIntField("num", 2).build();

    addPartition(partitioned, keyX1, "fileX1");
    addPartition(partitioned, keyY1, "fileY1");
    addPartition(partitioned, keyX2, "fileX2");
    addPartition(partitioned, keyY2, "fileY2");

    // verify that the partitions were added to Hive
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("str=x/num=1")),
                 new QueryResult(Lists.<Object>newArrayList("str=x/num=2")),
                 new QueryResult(Lists.<Object>newArrayList("str=y/num=1")),
                 new QueryResult(Lists.<Object>newArrayList("str=y/num=2"))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT count(*) AS count FROM " + tableName, true,
               Lists.newArrayList(new ColumnDesc("count", "BIGINT", 1, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(4L))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT * FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "STRING", 2, null),
                 new ColumnDesc(tableName + ".str", "STRING", 3, null),
                 new ColumnDesc(tableName + ".num", "INT", 4, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1", "x", 1)),
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2", "x", 2)),
                 new QueryResult(Lists.<Object>newArrayList("y1", "#1", "y", 1)),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2", "y", 2))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT * FROM " + tableName + " WHERE num = 2 ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "STRING", 2, null),
                 new ColumnDesc(tableName + ".str", "STRING", 3, null),
                 new ColumnDesc(tableName + ".num", "INT", 4, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2", "x", 2)),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2", "y", 2))));

    // drop a partition and query again
    dropPartition(partitioned, keyX2);

    // verify that one value is gone now, namely x2
    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("y1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetName);

    // verify the Hive table is gone
    runCommand("show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }

  @Test
  public void testTimePartitionedFileSet() throws Exception {

    final String datasetName = "parts";

    @SuppressWarnings("UnnecessaryLocalVariable")
    final String tableName = datasetName; // in this test context, the hive table name is the same as the dataset name

    // create a time partitioned file set
    datasetFramework.addInstance("timePartitionedFileSet", datasetName, FileSetProperties.builder()
      // properties for file set
      .setBasePath("/somePath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand("show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    TimePartitionedFileSet tpfs = datasetFramework.getDataset(datasetName, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(tpfs);
    Assert.assertTrue(tpfs instanceof TransactionAware);

    // add some partitions. Beware that Hive expects a partition to be a directory, so we create dirs with one file
    long time1 = DATE_FORMAT.parse("12/10/14 1:00 am").getTime();
    long time2 = DATE_FORMAT.parse("12/10/14 2:00 am").getTime();
    long time3 = DATE_FORMAT.parse("12/10/14 3:00 am").getTime();

    Location location1 = tpfs.getEmbeddedFileSet().getLocation("file1/nn");
    Location location2 = tpfs.getEmbeddedFileSet().getLocation("file2/nn");
    Location location3 = tpfs.getEmbeddedFileSet().getLocation("file3/nn");

    AvroHelper.generateAvroFile(location1.getOutputStream(), "x", 1, 2);
    AvroHelper.generateAvroFile(location2.getOutputStream(), "y", 2, 3);
    AvroHelper.generateAvroFile(location3.getOutputStream(), "x", 3, 4);

    addTimePartition(tpfs, time1, "file1");
    addTimePartition(tpfs, time2, "file2");
    addTimePartition(tpfs, time3, "file3");

    // verify that the partitions were added to Hive
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=2/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3")),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT key, value FROM " + tableName + " WHERE hour = 2 ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // remove a partition
    dropTimePartition(tpfs, time2);

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3"))));

    // verify the partition was removed from Hive
    // verify that the partitions were added to Hive
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetName);

    // verify the Hive table is gone
    runCommand("show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());

    datasetFramework.addInstance("timePartitionedFileSet", datasetName, FileSetProperties.builder()
      // properties for file set
      .setBasePath("/somePath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand("show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));
  }

  // this tests that the TPFS is backward-compatible after upgrade to 2.8, and correctly manages partitions in Hive.
  @Test
  public void testTimePartitionedFileSetBackwardsCompatibility() throws Exception {

    final String datasetName = "backward";

    @SuppressWarnings("UnnecessaryLocalVariable")
    final String tableName = datasetName; // in this test context, the hive table name is the same as the dataset name

    // create a time partitioned file set
    datasetFramework.addInstance("timePartitionedFileSet", datasetName, FileSetProperties.builder()
      // properties for file set
      .setBasePath("/somePath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand("show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations;
    TimePartitionedFileSet tpfs = datasetFramework.getDataset(
      datasetName,
      // this argument makes the tpfs behave as if it has legacy partitions
      ImmutableMap.of(TimePartitionedFileSetDataset.ARGUMENT_LEGACY_DATASET, "true"),
      null);
    Assert.assertNotNull(tpfs);
    Assert.assertTrue(tpfs instanceof TransactionAware);

    // add some partitions. Beware that Hive expects a partition to be a directory, so we create dirs with one file
    long time1 = DATE_FORMAT.parse("12/10/14 1:00 am").getTime();
    long time2 = DATE_FORMAT.parse("12/10/14 2:00 am").getTime();
    long time3 = DATE_FORMAT.parse("12/10/14 3:00 am").getTime();

    Location location1 = tpfs.getEmbeddedFileSet().getLocation("file1/nn");
    Location location2 = tpfs.getEmbeddedFileSet().getLocation("file2/nn");
    Location location3 = tpfs.getEmbeddedFileSet().getLocation("file3/nn");

    AvroHelper.generateAvroFile(location1.getOutputStream(), "x", 1, 2);
    AvroHelper.generateAvroFile(location2.getOutputStream(), "x", 2, 3);
    AvroHelper.generateAvroFile(location3.getOutputStream(), "x", 3, 4);

    addLegacyTimePartition(tpfs, time1, "file1");
    addLegacyTimePartition(tpfs, time3, "file3");

    // verify that the partitions were added to Hive
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3"))));

    // add a new partition with current code
    addTimePartition(tpfs, time2, "file2");

    // verify that the partition was added to Hive and we can query across all three
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=2/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3"))));

    // remove one of the legacy partitions
    dropTimePartition(tpfs, time3);

    // verify that the partition was added to Hive and we can query across all three
    runCommand("show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=2/minute=0"))));

    // verify that we can query the key-values in the file with Hive
    runCommand("SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2"))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetName);

    // verify the Hive table is gone
    runCommand("show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }

  private void addPartition(final PartitionedFileSet partitioned, final PartitionKey key, final String path)
    throws Exception {
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        partitioned.addPartition(key, path);
      }
    });
  }

  private void dropPartition(final PartitionedFileSet partitioned, final PartitionKey key) throws Exception {
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        partitioned.dropPartition(key);
      }
    });
  }

  private void addTimePartition(final TimePartitionedFileSet tpfs, final long time, final String path)
    throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.addPartition(time, path);
      }
    });
  }

  private void addLegacyTimePartition(final TimePartitionedFileSet partitioned,
                                      final long key, final String path)
    throws Exception {
    Assert.assertTrue(partitioned instanceof TimePartitionedFileSetDataset);
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        ((TimePartitionedFileSetDataset) partitioned).addLegacyPartition(key, path);
      }
    });
  }

  private void dropTimePartition(final TimePartitionedFileSet tpfs, final long time) throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.dropPartition(time);
      }
    });
  }

  private void doTransaction(Dataset tpfs, Runnable runnable) throws Exception {
    TransactionAware txAware = (TransactionAware) tpfs;
    Transaction tx = transactionManager.startShort(100);
    txAware.startTx(tx);
    runnable.run();
    Assert.assertTrue(txAware.commitTx());
    Assert.assertTrue(transactionManager.canCommit(tx, txAware.getTxChanges()));
    Assert.assertTrue(transactionManager.commit(tx));
    txAware.postTxCommit();
  }

}
