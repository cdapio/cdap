/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractHBaseTableUtilTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseTableUtilTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static CConfiguration cConf;
  private static HBaseTestBase testHBase;
  private static HBaseAdmin hAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    hAdmin = new HBaseAdmin(testHBase.getConfiguration());
    cConf = CConfiguration.create();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hAdmin.close();
    testHBase.stopHBase();
  }

  protected abstract HBaseTableUtil getTableUtil();

  protected abstract String getTableNameAsString(TableId tableId);

  protected abstract boolean namespacesSupported();

  @Test
  public void testTableSizeMetrics() throws Exception {
    HBaseTableUtil tableUtil = getTableUtil();
    // namespace should not exist
    if (namespacesSupported()) {
      Assert.assertFalse(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace")));
    }

    Assert.assertNull(getTableStats("namespace", "table1"));
    Assert.assertNull(getTableStats("namespace", "table2"));
    Assert.assertNull(getTableStats("namespace", "table3"));

    if (namespacesSupported()) {
      createNamespace("namespace");
      createNamespace("namespace2");
      Assert.assertTrue(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace")));
    }

    create("namespace", "table1");
    create("namespace2", "table1");
    create("namespace", "table2");
    create("namespace", "table3");

    Assert.assertTrue(exists("namespace", "table1"));
    Assert.assertTrue(exists("namespace2", "table1"));
    Assert.assertTrue(exists("namespace", "table2"));
    Assert.assertTrue(exists("namespace", "table3"));

    waitForMetricsToUpdate();

    Assert.assertEquals(0, getTableStats("namespace", "table1").getTotalSizeMB());
    Assert.assertEquals(0, getTableStats("namespace2", "table1").getTotalSizeMB());
    Assert.assertEquals(0, getTableStats("namespace", "table2").getTotalSizeMB());
    Assert.assertEquals(0, getTableStats("namespace", "table3").getTotalSizeMB());

    writeSome("namespace2", "table1");
    writeSome("namespace", "table2");
    writeSome("namespace", "table3");

    waitForMetricsToUpdate();

    Assert.assertEquals(0, getTableStats("namespace", "table1").getTotalSizeMB());
    Assert.assertTrue(getTableStats("namespace2", "table1").getTotalSizeMB() > 0);
    Assert.assertTrue(getTableStats("namespace", "table2").getTotalSizeMB() > 0);
    Assert.assertTrue(getTableStats("namespace", "table3").getTotalSizeMB() > 0);

    drop("namespace", "table1");
    Assert.assertFalse(exists("namespace", "table1"));
    //TODO: TestHBase methods should eventually accept namespace as a param, but will add them incrementally
    testHBase.forceRegionFlush(Bytes.toBytes(getTableNameAsString(TableId.from("namespace", "table2"))));
    truncate("namespace", "table3");

    waitForMetricsToUpdate();

    Assert.assertNull(getTableStats("namespace", "table1"));
    Assert.assertTrue(getTableStats("namespace", "table2").getTotalSizeMB() > 0);
    Assert.assertTrue(getTableStats("namespace", "table2").getStoreFileSizeMB() > 0);
    Assert.assertEquals(0, getTableStats("namespace", "table3").getTotalSizeMB());

    // modify
    HTableDescriptor desc = getTableDescriptor("namespace2", "table1");
    desc.setValue("mykey", "myvalue");
    disable("namespace2", "table1");
    getTableUtil().modifyTable(hAdmin, desc);
    desc = getTableDescriptor("namespace2", "table1");
    Assert.assertTrue(desc.getValue("mykey").equals("myvalue"));
    enable("namespace2", "table1");
    desc = getTableDescriptor("namespace", "table2");
    Assert.assertNull(desc.getValue("myKey"));

    if (namespacesSupported()) {
      try {
        deleteNamespace("namespace");
        Assert.fail("Should not be able to delete a non-empty namespace.");
      } catch (ConstraintException e) {
      }
    }

    drop("namespace2", "table1");
    drop("namespace", "table2");
    drop("namespace", "table3");

    if (namespacesSupported()) {
      deleteNamespace("namespace");
      deleteNamespace("namespace2");
      Assert.assertFalse(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace")));
      Assert.assertFalse(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace2")));
    }
  }

  @Test
  public void testBackwardCompatibility() throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    TableId tableId = TableId.from("cdap.default.my.dataset");
    create(tableId);
    Assert.assertEquals("default", HTableNameConverter.toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("cdap.user.my.dataset",
                        HTableNameConverter.getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(testHBase.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    tableId = TableId.from("cdap.default.system.queue.config");
    create(tableId);
    Assert.assertEquals("default", HTableNameConverter.toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("cdap.system.queue.config",
                        HTableNameConverter.getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(testHBase.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    tableId = TableId.from("cdap.myspace.could.be.any.table.name");
    createNamespace("myspace");
    create(tableId);
    Assert.assertEquals("cdap_myspace", HTableNameConverter.toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("could.be.any.table.name",
                        HTableNameConverter.getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(testHBase.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    deleteNamespace("myspace");
  }

  @Test
  public void testListAllInNamespace() throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    Set<TableId> fooNamespaceTableIds = ImmutableSet.of(TableId.from("cdap.foo.some.table1"),
                                                        TableId.from("cdap.foo.other.table"),
                                                        TableId.from("cdap.foo.some.table2"));
    createNamespace("foo");
    for (TableId tableId : fooNamespaceTableIds) {
      create(tableId);
    }

    createNamespace("foobar");
    TableId tableIdInOtherNamespace = TableId.from("cdap.foobar.my.dataset");
    create(tableIdInOtherNamespace);


    Set<TableId> retrievedTableIds =
      ImmutableSet.copyOf(tableUtil.listTablesInNamespace(hAdmin, Id.Namespace.from("foo")));
    Assert.assertEquals(fooNamespaceTableIds, retrievedTableIds);

    Set<TableId> allTableIds =
      ImmutableSet.<TableId>builder().addAll(fooNamespaceTableIds).add(tableIdInOtherNamespace).build();
    Assert.assertEquals(allTableIds, ImmutableSet.copyOf(tableUtil.listTables(hAdmin)));

    Assert.assertEquals(4, hAdmin.listTables().length);
    tableUtil.deleteAllInNamespace(hAdmin, Id.Namespace.from("foo"));
    Assert.assertEquals(1, hAdmin.listTables().length);

    drop(tableIdInOtherNamespace);
    Assert.assertEquals(0, hAdmin.listTables().length);
    deleteNamespace("foobar");
  }

  @Test
  public void testDropAllInDefaultNamespace() throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    TableId tableId = TableId.from("cdap.default.some.table1");
    create(tableId);

    tableId = TableId.from("cdap.default.other.table");
    create(tableId);

    tableId = TableId.from("cdap.default.some.table2");
    create(tableId);

    createNamespace("default2");
    TableId tableIdInOtherNamespace = TableId.from("cdap.default2.my.dataset");
    create(tableIdInOtherNamespace);

    Assert.assertEquals(4, hAdmin.listTables().length);
    tableUtil.deleteAllInNamespace(hAdmin, Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(1, hAdmin.listTables().length);

    drop(tableIdInOtherNamespace);
    Assert.assertEquals(0, hAdmin.listTables().length);
    deleteNamespace("default2");
  }

  @Test
  public void testDropAllInOtherNamespaceWithPrefix() throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    createNamespace("foonamespace");
    createNamespace("barnamespace");
    TableId tableId = TableId.from("cdap.foonamespace.some.table1");
    create(tableId);

    TableId tableIdWithOtherPrefix = TableId.from("cdap.foonamespace.other.table");
    create(tableIdWithOtherPrefix);

    tableId = TableId.from("cdap.foonamespace.some.table2");
    create(tableId);

    TableId tableIdInOtherNamespace = TableId.from("cdap.default.some.table1");
    create(tableIdInOtherNamespace);

    Assert.assertEquals(4, hAdmin.listTables().length);
    tableUtil.deleteAllInNamespace(hAdmin, Id.Namespace.from("foonamespace"), "some");
    Assert.assertEquals(2, hAdmin.listTables().length);

    drop(tableIdInOtherNamespace);
    drop(tableIdWithOtherPrefix);
    Assert.assertEquals(0, hAdmin.listTables().length);
    deleteNamespace("foonamespace");
    deleteNamespace("barnamespace");
  }

  private void waitForMetricsToUpdate() throws InterruptedException {
    // Wait for a bit to allow changes reflect in metrics: metrics updated on master with the heartbeat which
    // by default happens ~every 3 sec
    LOG.info("Waiting for metrics to reflect changes");
    TimeUnit.SECONDS.sleep(4);
  }

  private void writeSome(String namespace, String tableName) throws IOException {
    HTable table = getTableUtil().createHTable(testHBase.getConfiguration(), TableId.from(namespace, tableName));
    try {
      // writing at least couple megs to reflect in "megabyte"-based metrics
      for (int i = 0; i < 8; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("col" + i), new byte[1024 * 1024]);
        table.put(put);
      }
    } finally {
      table.close();
    }
  }

  private void createNamespace(String namespace) throws IOException {
    getTableUtil().createNamespaceIfNotExists(hAdmin, Id.Namespace.from(namespace));
  }

  private void deleteNamespace(String namespace) throws IOException {
    getTableUtil().deleteNamespaceIfExists(hAdmin, Id.Namespace.from(namespace));
  }

  private void create(String namespace, String tableName) throws IOException {
    create(TableId.from(namespace, tableName));
  }

  private void create(TableId tableId) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    HTableDescriptor desc = tableUtil.createHTableDescriptor(tableId);
    desc.addFamily(new HColumnDescriptor("d"));
    tableUtil.createTableIfNotExists(hAdmin, tableId, desc);
  }

  private boolean exists(String namespace, String tableName) throws IOException {
    return exists(TableId.from(namespace, tableName));
  }

  private boolean exists(TableId tableId) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    return tableUtil.tableExists(hAdmin, tableId);
  }

  private HTableDescriptor getTableDescriptor(String namespace, String name) throws IOException {
    return getTableUtil().getHTableDescriptor(hAdmin, TableId.from(namespace, name));
  }

  private void truncate(String namespace, String tableName) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    TableId tableId = TableId.from(namespace, tableName);
    tableUtil.truncateTable(hAdmin, tableId);
  }

  private void disable(String namespace, String tableName) throws IOException {
    getTableUtil().disableTable(hAdmin, TableId.from(namespace, tableName));
  }

  private void enable(String namespace, String tableName) throws IOException {
    getTableUtil().enableTable(hAdmin, TableId.from(namespace, tableName));
  }

  private void drop(String namespace, String tableName) throws IOException {
    drop(TableId.from(namespace, tableName));
  }

  private void drop(TableId tableId) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    tableUtil.dropTable(hAdmin, tableId);
  }

  private HBaseTableUtil.TableStats getTableStats(String namespace, String tableName) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    TableId tableId = TableId.from(namespace, tableName);
    return tableUtil.getTableStats(hAdmin).get(getTableNameAsString(tableId));
  }
}
