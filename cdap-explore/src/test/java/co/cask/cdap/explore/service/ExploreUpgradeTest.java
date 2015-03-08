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
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.hive.BaseHiveExploreService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
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

    // remove existing tables. will replace with manually created old-style tables
    waitForCompletion(Lists.newArrayList(
      exploreTableService.disableStream(Id.Stream.from(Constants.DEFAULT_NAMESPACE_ID, "purchases")),
      exploreTableService.disableDataset(kvID, datasetFramework.getDatasetSpec(kvID)),
      exploreTableService.disableDataset(filesetID, datasetFramework.getDatasetSpec(filesetID))));

    String createOldStream = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_stream_purchases " +
      "(ts bigint, headers map<string, string>, body string) COMMENT 'CDAP Stream' " +
      "STORED BY 'co.cask.cdap.hive.stream.StreamStorageHandler' " +
      "WITH SERDEPROPERTIES ('explore.stream.name'='purchases') " +
      //"WITH SERDEPROPERTIES ('explore.stream.name'='purchases', 'explore.stream.namespace'='default') " +
      "LOCATION '" + dummyPath + "' ";

    String createOldRecordScannable = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_kvtable " +
      "(key binary, value binary) COMMENT 'CDAP Dataset' " +
      "STORED BY 'co.cask.cdap.hive.datasets.DatasetStorageHandler' " +
      "WITH SERDEPROPERTIES ('explore.dataset.name'='kvtable')";
      //"WITH SERDEPROPERTIES ('explore.dataset.name'='kvtable', 'explore.dataset.namespace'='default')";

    String createOldFileset = "CREATE EXTERNAL TABLE IF NOT EXISTS cdap_myfiles " +
      "(body string, ts bigint) " +
      "PARTITIONED BY (year int, month int, day int, hour int, minute int) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
      "LOCATION '" + dummyPath + "' " +
      "TBLPROPERTIES ('cdap.name'='myfiles', 'avro.schema.literal'='" + schema.toString() + "')";

    String createNonCDAP = "CREATE TABLE some_table (x int, y string)";

    // WTF order matters... if you create a table from a dataset that uses DatasetStorageHandler,
    // the next tables will call initialize on DatasetStorageHandler. WHY?????!?!??
    waitForCompletion(Lists.newArrayList(
      ((BaseHiveExploreService) exploreService).execute("default", createNonCDAP),
      ((BaseHiveExploreService) exploreService).execute("default", createOldFileset),
      ((BaseHiveExploreService) exploreService).execute("default", createOldRecordScannable),
      ((BaseHiveExploreService) exploreService).execute("default", createOldStream)
    ));

    exploreService.upgrade();

    TableInfo tableInfo = exploreService.getTableInfo("default", "dataset_myfiles");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "dataset_kvtable");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "stream_purchases");
    Assert.assertTrue(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
    tableInfo = exploreService.getTableInfo("default", "some_table");
    Assert.assertFalse(tableInfo.getParameters().containsKey(Constants.Explore.CDAP_VERSION));
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
