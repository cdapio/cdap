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
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.DateFormat;
import java.util.Collections;

/**
 *
 */
@Category(SlowTests.class)
public class HiveExploreServiceFileSetTest extends BaseHiveExploreServiceTest {

  private static final Schema SCHEMA = Schema.recordOf("kv",
                                                       Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("value", Schema.of(Schema.Type.STRING)));
  private static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
  private static final String NAME = "parts";
  private static final String TABLE_NAME = NAME;

  @BeforeClass
  public static void start() throws Exception {
    startServices(CConfiguration.create(), false);
  }

  @AfterClass
  public static void deleteAll() throws Exception {
    datasetFramework.deleteAllInstances();
  }

  @Test
  public void testCreateAddDrop() throws Exception {

    // create a time partitioned file set
    datasetFramework.addInstance("timePartitionedFileSet", NAME, FileSetProperties.builder()
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
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(TABLE_NAME))));

    // Accessing dataset instance to perform data operations
    TimePartitionedFileSet tpfs = datasetFramework.getDataset(NAME, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(tpfs);
    Assert.assertTrue(tpfs instanceof TransactionAware);

    // add a few partitions
    long time1 = DATE_FORMAT.parse("12/10/14 1:00 am").getTime();
    long time2 = DATE_FORMAT.parse("12/10/14 2:00 am").getTime();
    long time3 = DATE_FORMAT.parse("12/10/14 3:00 am").getTime();
    addPartition(tpfs, time1, "file1");
    addPartition(tpfs, time2, "file2");
    addPartition(tpfs, time3, "file3");

    // verify that the partitions were added to Hive
    runCommand("show partitions " + TABLE_NAME, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=11/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=11/day=10/hour=2/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=11/day=10/hour=3/minute=0"))));

    // remove a partition
    dropPartition(tpfs, time2);

    // verify the partition was removed from Hive
    // verify that the partitions were added to Hive
    runCommand("show partitions " + TABLE_NAME, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=11/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=11/day=10/hour=3/minute=0"))));

    // drop the dataset
    datasetFramework.deleteInstance(NAME);

    // verify the Hive table is gone
    runCommand("show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());

    datasetFramework.addInstance("timePartitionedFileSet", NAME, FileSetProperties.builder()
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
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(TABLE_NAME))));
  }

  private void addPartition(final TimePartitionedFileSet tpfs, final long time, final String path) throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.addPartition(time, path);
      }
    });
  }

  private void dropPartition(final TimePartitionedFileSet tpfs, final long time) throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.dropPartition(time);
      }
    });
  }

  private void doTransaction(TimePartitionedFileSet tpfs, Runnable runnable) throws Exception {
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
