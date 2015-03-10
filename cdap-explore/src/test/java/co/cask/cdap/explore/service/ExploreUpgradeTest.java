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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.hive.BaseHiveExploreService;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests explore metadata endpoints.
 */
@Category(SlowTests.class)
public class ExploreUpgradeTest extends BaseHiveExploreServiceTest {

  @BeforeClass
  public static void start() throws Exception {
    initialize();
  }

  @Test
  public void testUpgrade() throws Exception {
    // add some old style tables to default database
    String dummyPath = tmpFolder.newFolder().getAbsolutePath();

    // create a stream and some datasets that will be upgraded. Need to create the actual instances
    // so that upgrade can find them, but we will manually create the Hive tables for them in the old style.

    // add a stream
    createStream("purchases");

    // add a key-value table for record scannables
    Id.DatasetInstance kvID = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, "kvtable");
    datasetFramework.addInstance(KeyValueTable.class.getName(), kvID, DatasetProperties.EMPTY);

    // add a time partitioned fileset
    Id.DatasetInstance filesetID = Id.DatasetInstance.from(Constants.DEFAULT_NAMESPACE_ID, "myfiles");
    Schema schema = Schema.recordOf("rec",
                                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)));
    datasetFramework.addInstance(TimePartitionedFileSet.class.getName(), filesetID, FileSetProperties.builder()
      .setBasePath("/my/path")
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema.toString())
      .build());
    TimePartitionedFileSet tpfs = datasetFramework.getDataset(filesetID, Collections.<String, String>emptyMap(), null);
    Transaction tx1 = transactionManager.startShort(100);
    TransactionAware txTpfs = (TransactionAware) tpfs;
    txTpfs.startTx(tx1);
    tpfs.addPartition(0L, "epoch");
    Map<PartitionKey, String> partitions = tpfs.getPartitions(null);
    txTpfs.commitTx();
    transactionManager.canCommit(tx1, txTpfs.getTxChanges());
    transactionManager.commit(tx1);
    txTpfs.postTxCommit();

    // remove existing tables. will replace with manually created old-style tables
    waitForCompletion(Lists.newArrayList(
      exploreTableService.disableStream(Id.Stream.from(Constants.DEFAULT_NAMESPACE_ID, "purchases")),
      exploreTableService.disableDataset(kvID, datasetFramework.getDatasetSpec(kvID)),
      exploreTableService.disableDataset(filesetID, datasetFramework.getDatasetSpec(filesetID))));

    String createOldStream = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_stream_purchases " +
      "(ts bigint, headers map<string, string>, body string) COMMENT 'CDAP Stream' " +
      "STORED BY 'co.cask.cdap.hive.stream.StreamStorageHandler' " +
      "WITH SERDEPROPERTIES ('explore.stream.name'='purchases') " +
      "LOCATION '" + dummyPath + "' ";

    String createOldRecordScannable = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_kvtable " +
      "(key binary, value binary) COMMENT 'CDAP Dataset' " +
      "STORED BY 'co.cask.cdap.hive.datasets.DatasetStorageHandler' " +
      "WITH SERDEPROPERTIES ('explore.dataset.name'='cdap.user.kvtable')";

    String createOldFileset = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_myfiles " +
      "(body string, ts bigint) " +
      "PARTITIONED BY (year int, month int, day int, hour int, minute int) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "LOCATION '" + dummyPath + "' " +
      "TBLPROPERTIES ('cdap.name'='cdap.user.myfiles', 'avro.schema.literal'='" + schema.toString() + "')";

    String createNonCDAP = "CREATE TABLE some_table (x int, y string)";

    // order matters, at least in unit test environment...
    // if you create a table from a dataset that uses DatasetStorageHandler,
    // the next tables will call initialize on DatasetStorageHandler...
    // TODO: find out why the above is true
    waitForCompletion(Lists.newArrayList(
      ((BaseHiveExploreService) exploreService).execute("default", createNonCDAP),
      ((BaseHiveExploreService) exploreService).execute("default", createOldFileset),
      ((BaseHiveExploreService) exploreService).execute("default", createOldRecordScannable),
      ((BaseHiveExploreService) exploreService).execute("default", createOldStream)
    ));

    exploreService.upgrade();

    // check new tables have cdap version, which means they were upgraded
    TableInfo tableInfo = exploreService.getTableInfo("default", "dataset_myfiles");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "dataset_kvtable");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "stream_purchases");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "some_table");
    Assert.assertFalse(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));

    // check partition was added to tpfs dataset
    Iterator<PartitionKey> partitionKeyIter = partitions.keySet().iterator();
    String expected = stringify(partitionKeyIter.next());
    runCommand(Constants.DEFAULT_NAMESPACE_ID, "show partitions dataset_myfiles", true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(expected)))
    );


    // check that old tables were dropped
    Assert.assertEquals(4, exploreService.getTables("default").size());
  }

  private String stringify(PartitionKey partitionKey) {
    return String.format("year=%s/month=%s/day=%s/hour=%s/minute=%s",
                         partitionKey.getField("year"),
                         partitionKey.getField("month"),
                         partitionKey.getField("day"),
                         partitionKey.getField("hour"),
                         partitionKey.getField("minute"));
  }

  private void waitForCompletion(List<QueryHandle> handles) throws Exception {
    for (QueryHandle handle : handles) {
      QueryStatus status = exploreService.getStatus(handle);
      while (!status.getStatus().isDone()) {
        TimeUnit.SECONDS.sleep(1);
        status = exploreService.getStatus(handle);
      }
    }
  }
}
