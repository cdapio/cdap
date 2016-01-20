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
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractHBaseTableUtilTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  protected static CConfiguration cConf;
  private static HBaseAdmin hAdmin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    hAdmin = new HBaseAdmin(TEST_HBASE.getConfiguration());
    cConf = CConfiguration.create();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hAdmin.close();
  }

  protected abstract HBaseTableUtil getTableUtil();

  protected abstract HTableNameConverter getNameConverter();

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

    Futures.allAsList(
      createAsync(TableId.from("namespace", "table1")),
      createAsync(TableId.from("namespace2", "table1")),
      createAsync(TableId.from("namespace", "table2")),
      createAsync(TableId.from("namespace", "table3"))
    ).get(60, TimeUnit.SECONDS);

    Assert.assertTrue(exists("namespace", "table1"));
    Assert.assertTrue(exists("namespace2", "table1"));
    Assert.assertTrue(exists("namespace", "table2"));
    Assert.assertTrue(exists("namespace", "table3"));

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          Assert.assertEquals(0, getTableStats("namespace", "table1").getTotalSizeMB());
          Assert.assertEquals(0, getTableStats("namespace2", "table1").getTotalSizeMB());
          Assert.assertEquals(0, getTableStats("namespace", "table2").getTotalSizeMB());
          Assert.assertEquals(0, getTableStats("namespace", "table3").getTotalSizeMB());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    writeSome("namespace2", "table1");
    writeSome("namespace", "table2");
    writeSome("namespace", "table3");

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          Assert.assertEquals(0, getTableStats("namespace", "table1").getTotalSizeMB());
          Assert.assertTrue(getTableStats("namespace2", "table1").getTotalSizeMB() > 0);
          Assert.assertTrue(getTableStats("namespace", "table2").getTotalSizeMB() > 0);
          Assert.assertTrue(getTableStats("namespace", "table3").getTotalSizeMB() > 0);
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    drop("namespace", "table1");
    Assert.assertFalse(exists("namespace", "table1"));
    //TODO: TestHBase methods should eventually accept namespace as a param, but will add them incrementally
    TEST_HBASE.forceRegionFlush(Bytes.toBytes(getTableNameAsString(TableId.from("namespace", "table2"))));
    truncate("namespace", "table3");

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          Assert.assertNull(getTableStats("namespace", "table1"));
          Assert.assertTrue(getTableStats("namespace", "table2").getTotalSizeMB() > 0);
          Assert.assertTrue(getTableStats("namespace", "table2").getStoreFileSizeMB() > 0);
          Assert.assertEquals(0, getTableStats("namespace", "table3").getTotalSizeMB());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // modify
    HTableDescriptor desc = getTableDescriptor("namespace2", "table1");
    HTableDescriptorBuilder newDesc = getTableUtil().buildHTableDescriptor(desc);
    newDesc.setValue("mykey", "myvalue");
    disable("namespace2", "table1");
    getTableUtil().modifyTable(hAdmin, newDesc.build());
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
        // Expected exception
      }
    }

    Futures.allAsList(
      dropAsync(TableId.from("namespace2", "table1")),
      dropAsync(TableId.from("namespace", "table2")),
      dropAsync(TableId.from("namespace", "table3"))
    ).get(60, TimeUnit.SECONDS);

    if (namespacesSupported()) {
      deleteNamespace("namespace");
      deleteNamespace("namespace2");
      Assert.assertFalse(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace")));
      Assert.assertFalse(tableUtil.hasNamespace(hAdmin, Id.Namespace.from("namespace2")));
    }
  }

  @Test
  public void testBackwardCompatibility() throws IOException, InterruptedException {
    HBaseTableUtil tableUtil = getTableUtil();
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    TableId tableId = TableId.from("default", "my.dataset");
    create(tableId);
    Assert.assertEquals("default", getNameConverter().toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("cdap.user.my.dataset",
                        getNameConverter().getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(TEST_HBASE.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    tableId = TableId.from("default", "system.queue.config");
    create(tableId);
    Assert.assertEquals("default", getNameConverter().toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("cdap.system.queue.config",
                        getNameConverter().getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(TEST_HBASE.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    tableId = TableId.from("myspace", "could.be.any.table.name");
    createNamespace("myspace");
    create(tableId);
    Assert.assertEquals("cdap_myspace", getNameConverter().toHBaseNamespace(tablePrefix, tableId.getNamespace()));
    Assert.assertEquals("could.be.any.table.name",
                        getNameConverter().getHBaseTableName(tablePrefix, tableId));
    Assert.assertEquals(getTableNameAsString(tableId),
                        Bytes.toString(tableUtil.createHTable(TEST_HBASE.getConfiguration(), tableId).getTableName()));
    drop(tableId);
    deleteNamespace("myspace");
  }

  @Test
  public void testListAllInNamespace() throws Exception {
    HBaseTableUtil tableUtil = getTableUtil();
    Set<TableId> fooNamespaceTableIds = ImmutableSet.of(TableId.from("foo", "some.table1"),
                                                        TableId.from("foo", "other.table"),
                                                        TableId.from("foo", "some.table2"));
    createNamespace("foo");
    createNamespace("foo_bar");
    TableId tableIdInOtherNamespace = TableId.from("foo_bar", "my.dataset");

    List<ListenableFuture<TableId>> createFutures = new ArrayList<>();
    for (TableId tableId : fooNamespaceTableIds) {
      createFutures.add(createAsync(tableId));
    }

    createFutures.add(createAsync(tableIdInOtherNamespace));

    Futures.allAsList(createFutures).get(60, TimeUnit.SECONDS);

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
    deleteNamespace("foo_bar");
  }

  @Test
  public void testDropAllInDefaultNamespace() throws Exception {
    HBaseTableUtil tableUtil = getTableUtil();

    TableId tableIdInOtherNamespace = TableId.from("default2", "my.dataset");
    createNamespace("default2");

    Futures.allAsList(
      createAsync(TableId.from("default", "some.table1")),
      createAsync(TableId.from("default", "other.table")),
      createAsync(TableId.from("default", "some.table2")),
      createAsync(tableIdInOtherNamespace)
    ).get(60, TimeUnit.SECONDS);

    Assert.assertEquals(4, hAdmin.listTables().length);
    tableUtil.deleteAllInNamespace(hAdmin, Id.Namespace.DEFAULT);
    Assert.assertEquals(1, hAdmin.listTables().length);

    drop(tableIdInOtherNamespace);
    Assert.assertEquals(0, hAdmin.listTables().length);
    deleteNamespace("default2");
  }

  @Test
  public void testDropAllInOtherNamespaceWithPrefix() throws Exception {
    HBaseTableUtil tableUtil = getTableUtil();
    createNamespace("foonamespace");
    createNamespace("barnamespace");

    TableId tableIdWithOtherPrefix = TableId.from("foonamespace", "other.table");
    TableId tableIdInOtherNamespace = TableId.from("default", "some.table1");

    Futures.allAsList(
      createAsync(TableId.from("foonamespace", "some.table1")),
      createAsync(tableIdWithOtherPrefix),
      createAsync(TableId.from("foonamespace", "some.table2")),
      createAsync(tableIdInOtherNamespace)
    ).get(60, TimeUnit.SECONDS);

    Assert.assertEquals(4, hAdmin.listTables().length);
    tableUtil.deleteAllInNamespace(hAdmin, Id.Namespace.from("foonamespace"), new Predicate<TableId>() {
      @Override
      public boolean apply(TableId input) {
        return input.getTableName().startsWith("some");
      }
    });
    Assert.assertEquals(2, hAdmin.listTables().length);

    Futures.allAsList(
      dropAsync(tableIdInOtherNamespace),
      dropAsync(tableIdWithOtherPrefix)
    ).get(60, TimeUnit.SECONDS);

    Assert.assertEquals(0, hAdmin.listTables().length);
    deleteNamespace("foonamespace");
    deleteNamespace("barnamespace");
  }

  private void writeSome(String namespace, String tableName) throws IOException {
    try (HTable table = getTableUtil().createHTable(TEST_HBASE.getConfiguration(),
                                                    TableId.from(namespace, tableName))) {
      // writing at least couple megs to reflect in "megabyte"-based metrics
      for (int i = 0; i < 8; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        put.add(Bytes.toBytes("d"), Bytes.toBytes("col" + i), new byte[1024 * 1024]);
        table.put(put);
      }
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
    HTableDescriptorBuilder desc = tableUtil.buildHTableDescriptor(tableId);
    desc.addFamily(new HColumnDescriptor("d"));
    tableUtil.createTableIfNotExists(hAdmin, tableId, desc.build());
  }

  private ListenableFuture<TableId> createAsync(final TableId tableId) {
    final SettableFuture<TableId> future = SettableFuture.create();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          create(tableId);
          future.set(tableId);
        } catch (Throwable t) {
          future.setException(t);
        }
      }
    };
    t.start();
    return future;
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

  private ListenableFuture<TableId> dropAsync(final TableId tableId) {
    final SettableFuture<TableId> future = SettableFuture.create();
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          drop(tableId);
          future.set(tableId);
        } catch (Throwable t) {
          future.setException(t);
        }
      }
    };
    t.start();
    return future;
  }

  private HBaseTableUtil.TableStats getTableStats(String namespace, String tableName) throws IOException {
    HBaseTableUtil tableUtil = getTableUtil();
    // todo : should support custom table-prefix
    TableId tableId = TableId.from(namespace, tableName);
    Map<TableId, HBaseTableUtil.TableStats> statsMap = tableUtil.getTableStats(hAdmin);
    return statsMap.get(tableId);
  }
}
