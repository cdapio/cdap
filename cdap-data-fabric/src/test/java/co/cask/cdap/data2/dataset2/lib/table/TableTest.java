/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.metrics.MeteredDataset;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.metrics.MetricsCollector;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.TableAssert;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTable;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Base test for Table.
 * @param <T> table type
 */
public abstract class TableTest<T extends Table> {
  static final byte[] R1 = Bytes.toBytes("r1");
  static final byte[] R2 = Bytes.toBytes("r2");
  static final byte[] R3 = Bytes.toBytes("r3");
  static final byte[] R4 = Bytes.toBytes("r4");
  static final byte[] R5 = Bytes.toBytes("r5");

  static final byte[] C1 = Bytes.toBytes("c1");
  static final byte[] C2 = Bytes.toBytes("c2");
  static final byte[] C3 = Bytes.toBytes("c3");
  static final byte[] C4 = Bytes.toBytes("c4");
  static final byte[] C5 = Bytes.toBytes("c5");

  static final byte[] V1 = Bytes.toBytes("v1");
  static final byte[] V2 = Bytes.toBytes("v2");
  static final byte[] V3 = Bytes.toBytes("v3");
  static final byte[] V4 = Bytes.toBytes("v4");
  static final byte[] V5 = Bytes.toBytes("v5");

  static final byte[] L1 = Bytes.toBytes(1L);
  static final byte[] L2 = Bytes.toBytes(2L);
  static final byte[] L3 = Bytes.toBytes(3L);
  static final byte[] L4 = Bytes.toBytes(4L);
  static final byte[] L5 = Bytes.toBytes(5L);

  static final byte[] MT = new byte[0];

  protected static final NamespaceId NAMESPACE1 = new NamespaceId("ns1");
  protected static final NamespaceId NAMESPACE2 = new NamespaceId("ns2");
  protected static final String MY_TABLE = "myTable";
  protected static final DatasetContext CONTEXT1 = DatasetContext.from(NAMESPACE1.getEntityName());
  protected static final DatasetContext CONTEXT2 = DatasetContext.from(NAMESPACE2.getEntityName());

  static final DatasetProperties PROPS_CONFLICT_LEVEL_ROW = DatasetProperties.builder()
    .add(Table.PROPERTY_CONFLICT_LEVEL, ConflictDetection.ROW.name()).build();

  protected TransactionSystemClient txClient;

  protected abstract T getTable(DatasetContext datasetContext, String name,
                                DatasetProperties props, Map<String, String> runtimeArguments) throws Exception;
  protected abstract DatasetAdmin getTableAdmin(DatasetContext datasetContext, String name,
                                                DatasetProperties props) throws Exception;

  protected abstract boolean isReadlessIncrementSupported();

  protected T getTable(DatasetContext datasetContext, String name, DatasetProperties props) throws Exception {
    return getTable(datasetContext, name, props, Collections.<String, String>emptyMap());
  }

  protected T getTable(DatasetContext datasetContext, String name) throws Exception {
    return getTable(datasetContext, name, PROPS_CONFLICT_LEVEL_ROW);
  }

  protected DatasetAdmin getTableAdmin(DatasetContext datasetContext, String name) throws Exception {
    return getTableAdmin(datasetContext, name, PROPS_CONFLICT_LEVEL_ROW);
  }

  @Before
  public void before() {
    Configuration txConf = HBaseConfiguration.create();
    TransactionManager txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);
  }

  @After
  public void after() {
    txClient = null;
  }

  @Test
  public void testCreate() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    Assert.assertFalse(admin.exists());
    admin.create();
    Assert.assertTrue(admin.exists());
    // creation of non-existing table should do nothing
    admin.create();
    admin.drop();
  }

  // TODO figure out what to do with this. As long as ObjectMappedTable writes empty values, we cannot
  //      throw exceptions, and this test is pointless.
  @Ignore
  @Test
  public void testEmptyValuePut() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    Transaction tx = txClient.startShort();
    try {
      Table myTable = getTable(CONTEXT1, MY_TABLE);
      try {
        myTable.put(R1, C1, MT);
        Assert.fail("Put with empty value should fail.");
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        myTable.put(R1, a(C1, C2), a(V1, MT));
        Assert.fail("Put with empty value should fail.");
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        myTable.put(new Put(R1).add(C1, V1).add(C2, MT));
        Assert.fail("Put with empty value should fail.");
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        myTable.compareAndSwap(R1, C1, V1, MT);
        Assert.fail("CompareAndSwap with empty value should fail.");
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      txClient.abort(tx);
      admin.drop();
    }
  }

  @Test
  public void testEmptyGet() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx = txClient.startShort();
      Table myTable = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable).startTx(tx);

      myTable.put(R1, C1, V1);
      myTable.put(R1, C2, V2);
      // to be used for validation later
      TreeMap<byte[], byte[]> expectedColumns = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      expectedColumns.put(C1, V1);
      expectedColumns.put(C2, V2);
      Result expectedResult = new Result(R1, expectedColumns);

      Result emptyResult = new Result(R1, ImmutableMap.<byte[], byte[]>of());

      ((TransactionAware) myTable).commitTx();
      txClient.commit(tx);

      // start another transaction, so that the buffering table doesn't cache the values; the underlying Table
      // implementations are tested this way.
      tx = txClient.startShort();
      ((TransactionAware) myTable).startTx(tx);


      Row row = myTable.get(R1, new byte[][]{ C1, C2 });
      assertEquals(expectedResult, row);

      // passing in empty columns returns empty result
      row = myTable.get(R1, new byte[][]{});
      assertEquals(emptyResult, row);


      // test all the Get constructors and their behavior

      // constructors specifying only rowkey retrieve all columns
      Get get = new Get(R1);
      Assert.assertNull(get.getColumns());
      assertEquals(expectedResult, myTable.get(get));

      get = new Get(Bytes.toString(R1));
      Assert.assertNull(get.getColumns());
      assertEquals(expectedResult, myTable.get(get));

      get.add(C1);
      get.add(Bytes.toString(C2));
      assertEquals(expectedResult, myTable.get(get));

      // constructor specifying columns, but with an empty array/collection retrieve 0 columns
      get = new Get(R1, new byte[][]{});
      Assert.assertNotNull(get.getColumns());
      assertEquals(emptyResult, myTable.get(get));

      get = new Get(R1, ImmutableList.<byte[]>of());
      Assert.assertNotNull(get.getColumns());
      assertEquals(emptyResult, myTable.get(get));

      get = new Get(Bytes.toString(R1), new String[]{});
      Assert.assertNotNull(get.getColumns());
      assertEquals(emptyResult, myTable.get(get));

      get = new Get(Bytes.toString(R1), ImmutableList.<String>of());
      Assert.assertNotNull(get.getColumns());
      assertEquals(emptyResult, myTable.get(get));

      row = myTable.get(R1, new byte[][]{ });
      assertEquals(emptyResult, row);

      txClient.abort(tx);
    } finally {
      admin.drop();
    }
  }

  private void assertEquals(Row expected, Row actual) {
    Assert.assertArrayEquals(expected.getRow(), actual.getRow());
    Assert.assertTrue(columnsEqual(expected.getColumns(), actual.getColumns()));
  }

  // AbstractMap#equals doesn't work, because the entries are byte[]
  private boolean columnsEqual(Map<byte[], byte[]> expected, Map<byte[], byte[]> actual) {
    if (expected.size() != actual.size()) {
      return false;
    }

    for (Map.Entry<byte[], byte[]> entry : expected.entrySet()) {
      byte[] key = entry.getKey();
      byte[] value = entry.getValue();
      if (value == null) {
        if (!(actual.get(key) == null && actual.containsKey(key))) {
          return false;
        }
      } else {
        if (!Arrays.equals(value, actual.get(key))) {
          return false;
        }
      }
    }
    return true;
  }

  @Test
  public void testEmptyDelete() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx = txClient.startShort();
      Table myTable = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable).startTx(tx);

      myTable.put(R1, C1, V1);
      myTable.put(R1, C2, V2);
      myTable.put(R1, C3, V3);

      // specifying empty columns means to delete nothing
      myTable.delete(R1, new byte[][]{});
      myTable.delete(new Delete(R1, new byte[][]{}));
      myTable.delete(new Delete(R1, ImmutableList.<byte[]>of()));
      myTable.delete(new Delete(Bytes.toString(R1), new String[]{}));
      myTable.delete(new Delete(Bytes.toString(R1), ImmutableList.<String>of()));

      // verify the above delete calls deleted none of the rows
      Row row = myTable.get(R1);
      Assert.assertEquals(3, row.getColumns().size());
      Assert.assertArrayEquals(R1, row.getRow());
      Assert.assertArrayEquals(V1, row.get(C1));
      Assert.assertArrayEquals(V2, row.get(C2));
      Assert.assertArrayEquals(V3, row.get(C3));


      // test deletion of only one column
      Delete delete = new Delete(R1);
      Assert.assertNull(delete.getColumns());
      delete.add(C1);
      Assert.assertNotNull(delete.getColumns());
      myTable.delete(delete);

      row = myTable.get(R1);
      Assert.assertEquals(2, row.getColumns().size());
      Assert.assertArrayEquals(R1, row.getRow());
      Assert.assertArrayEquals(V2, row.get(C2));
      Assert.assertArrayEquals(V3, row.get(C3));

      // test delete of all columns
      myTable.delete(new Delete(R1));
      Assert.assertEquals(0, myTable.get(R1).getColumns().size());

      txClient.abort(tx);
    } finally {
      admin.drop();
    }
  }

  @Test
  public void testMultiGetWithEmpty() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx = txClient.startShort();
      Table myTable = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable).startTx(tx);

      myTable.put(R1, C1, V1);
      myTable.put(R1, C2, V2);
      myTable.put(R1, C3, V3);
      myTable.put(R1, C4, V4);

      List<Get> gets = new ArrayList<>();
      // the second and fourth Gets are requesting 0 columns. This tests correctness of batch-get logic, when there
      // is/are empty Gets among them.
      gets.add(new Get(R1, C1));
      gets.add(new Get(R1, ImmutableList.<byte[]>of()));
      gets.add(new Get(R1, C2, C3));
      gets.add(new Get(R1, ImmutableList.<byte[]>of()));
      gets.add(new Get(R1, C4));

      List<Row> rows = myTable.get(gets);
      // first off, the Gets at index two and four should be empty
      Assert.assertEquals(0, rows.get(1).getColumns().size());
      Assert.assertEquals(0, rows.get(3).getColumns().size());

      // verify the results of the other Gets
      Assert.assertEquals(1, rows.get(0).getColumns().size());
      Assert.assertArrayEquals(V1, rows.get(0).get(C1));
      Assert.assertEquals(2, rows.get(2).getColumns().size());
      Assert.assertArrayEquals(V2, rows.get(2).get(C2));
      Assert.assertArrayEquals(V3, rows.get(2).get(C3));
      Assert.assertEquals(1, rows.get(4).getColumns().size());
      Assert.assertArrayEquals(V4, rows.get(4).get(C4));

      txClient.abort(tx);
    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicGetPutWithTx() throws Exception {

    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // TableAssert.verify can see changes inside tx
      TableAssert.assertRow(a(C1, V1), myTable1.get(R1, a(C1, C2)).getColumns());
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(null, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R2, C1));
      TableAssert.assertRow(a(C1, V1), myTable1.get(R1).getColumns());

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.startShort();
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      // TableAssert.verify doesn't see changes of tx1
      TableAssert.assertRow(a(), myTable2.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable2.get(R1, C1));
      Assert.assertArrayEquals(null, myTable2.get(R1, C2));
      TableAssert.assertRow(a(), myTable2.get(R1));
      // write r2->c2,v2 in tx2
      myTable2.put(R2, a(C2), a(V2));
      // TableAssert.verify can see own changes
      TableAssert.assertRow(a(C2, V2), myTable2.get(R2, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable2.get(R2, C1));
      Assert.assertArrayEquals(V2, myTable2.get(R2, C2));
      TableAssert.assertRow(a(C2, V2), myTable2.get(R2));

      // TableAssert.verify tx1 cannot see changes of tx2
      TableAssert.assertRow(a(), myTable1.get(R2, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable1.get(R2, C1));
      Assert.assertArrayEquals(null, myTable1.get(R2, C2));
      TableAssert.assertRow(a(), myTable1.get(R2));

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // start tx3 and TableAssert.verify that changes of tx1 are not visible yet (even though they are flushed)
      Transaction tx3 = txClient.startShort();
      Table myTable3 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable3).startTx(tx3);
      TableAssert.assertRow(a(), myTable3.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable3.get(R1, C1));
      Assert.assertArrayEquals(null, myTable3.get(R1, C2));
      TableAssert.assertRow(a(), myTable3.get(R1));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));
      // start tx4 and TableAssert.verify that changes of tx1 are now visible
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx4 = txClient.startShort();
      ((TransactionAware) myTable3).startTx(tx4);
      TableAssert.assertRow(a(C1, V1), myTable3.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable3.get(R1, C1));
      Assert.assertArrayEquals(null, myTable3.get(R1, C2));
      TableAssert.assertRow(a(C1, V1), myTable3.get(R1));

      // but tx2 still doesn't see committed changes of tx2
      TableAssert.assertRow(a(), myTable2.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable2.get(R1, C1));
      Assert.assertArrayEquals(null, myTable2.get(R1, C2));
      TableAssert.assertRow(a(), myTable2.get(R1));
      // and tx4 doesn't see changes of tx2
      TableAssert.assertRow(a(), myTable3.get(R2, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable3.get(R2, C1));
      Assert.assertArrayEquals(null, myTable3.get(R2, C2));
      TableAssert.assertRow(a(), myTable3.get(R2));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // do change in tx2 that is conflicting with tx1
      myTable2.put(R1, a(C1), a(V2));
      // change is OK and visible inside tx2
      TableAssert.assertRow(a(C1, V2), myTable2.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V2, myTable2.get(R1, C1));
      Assert.assertArrayEquals(null, myTable2.get(R1, C2));
      TableAssert.assertRow(a(C1, V2), myTable2.get(R1));

      // cannot commit: conflict should be detected
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));

      // rolling back tx2 changes and aborting tx
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // TableAssert.verifying that none of the changes of tx2 made it to be visible to other txs
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx5 = txClient.startShort();
      ((TransactionAware) myTable3).startTx(tx5);
      TableAssert.assertRow(a(C1, V1), myTable3.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable3.get(R1, C1));
      Assert.assertArrayEquals(null, myTable3.get(R1, C2));
      TableAssert.assertRow(a(C1, V1), myTable3.get(R1));
      TableAssert.assertRow(a(), myTable3.get(R2, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable3.get(R2, C1));
      Assert.assertArrayEquals(null, myTable3.get(R2, C2));
      TableAssert.assertRow(a(), myTable3.get(R2));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicCompareAndSwapWithTx() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // write r1->c2,v2 but not commit
      Assert.assertTrue(myTable1.compareAndSwap(R1, C2, null, V5));
      // TableAssert.verify compare and swap result visible inside tx before commit
      TableAssert.assertRow(a(C1, V1, C2, V5), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(V5, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, V1, C2, V5), myTable1.get(R1));
      // these should fail
      Assert.assertFalse(myTable1.compareAndSwap(R1, C1, null, V1));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C1, V2, V1));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C2, null, V2));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C2, V2, V1));
      // but this should succeed
      Assert.assertTrue(myTable1.compareAndSwap(R1, C2, V5, V2));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.startShort();

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // check that tx2 doesn't see changes (even though they were flushed) of tx1 by trying to compareAndSwap
      // assuming current value is null
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      Assert.assertTrue(myTable2.compareAndSwap(R1, C1, null, V3));

      // start tx3 and TableAssert.verify same thing again
      Transaction tx3 = txClient.startShort();
      Table myTable3 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable3).startTx(tx3);
      Assert.assertTrue(myTable3.compareAndSwap(R1, C1, null, V2));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // TableAssert.verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // start tx4 and TableAssert.verify that changes of tx1 are now visible
      Transaction tx4 = txClient.startShort();
      Table myTable4 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable4).startTx(tx4);
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable4.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable4.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable4.get(R1, C2));
      Assert.assertArrayEquals(null, myTable4.get(R1, C3));
      TableAssert.assertRow(a(C2, V2), myTable4.get(R1, a(C2)));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable4.get(R1));

      // tx3 still cannot see tx1 changes
      Assert.assertTrue(myTable3.compareAndSwap(R1, C2, null, V5));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      ((TransactionAware) myTable3).rollbackTx();
      txClient.abort(tx3);

      // TableAssert.verify we can do some ops with tx4 based on data written with tx1
      Assert.assertFalse(myTable4.compareAndSwap(R1, C1, null, V4));
      Assert.assertFalse(myTable4.compareAndSwap(R1, C2, null, V5));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C1, V1, V3));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C2, V2, V4));
      myTable4.delete(R1, a(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // TableAssert.verifying the result contents in next transaction
      Transaction tx5 = txClient.startShort();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      TableAssert.assertRow(a(C2, V4), myTable4.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable4.get(R1, C1));
      Assert.assertArrayEquals(V4, myTable4.get(R1, C2));
      TableAssert.assertRow(a(C2, V4), myTable4.get(R1));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicIncrementWithTx() throws Exception {
    testBasicIncrementWithTx(true, false); // incrementAndGet(), readless increments off
    testBasicIncrementWithTx(false, false); // increment(), readless increments off
    if (isReadlessIncrementSupported()) {
      testBasicIncrementWithTx(false, true); // increment(), readless increments on
    }
  }

  private void testBasicIncrementWithTx(boolean doIncrAndGet, boolean readless) throws Exception {
    DatasetProperties props = DatasetProperties.builder().add(
      Table.PROPERTY_READLESS_INCREMENT, String.valueOf(readless)).build();
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE, props);
    admin.create();
    Table myTable1, myTable2, myTable3, myTable4;
    try {
      Transaction tx1 = txClient.startShort();
      myTable1 = getTable(CONTEXT1, MY_TABLE, props);
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1), a(L4));
      doIncrement(doIncrAndGet, myTable1, R1, a(C1), la(-3L), lb(1L));
      doIncrement(doIncrAndGet, myTable1, R1, a(C2), la(2L), lb(2L));
      // TableAssert.verify increment result visible inside tx before commit
      TableAssert.assertRow(a(C1, L1, C2, L2), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(L1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(L2, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, L1), myTable1.get(R1, a(C1)));
      TableAssert.assertRow(a(C1, L1, C2, L2), myTable1.get(R1));
      // incrementing non-long value should fail
      myTable1.put(R1, a(C5), a(V5));
      try {
        doIncrement(doIncrAndGet, myTable1, R1, a(C5), la(5L), lb(-1L));
        Assert.fail("increment should have failed with NumberFormatException");
      } catch (NumberFormatException e) {
        // Expected
      }
      // previous increment should not do any change
      TableAssert.assertRow(a(C5, V5), myTable1.get(R1, a(C5)));
      Assert.assertArrayEquals(V5, myTable1.get(R1, C5));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.startShort();

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // check that tx2 doesn't see changes (even though they were flushed) of tx1
      // assuming current value is null
      myTable2 = getTable(CONTEXT1, MY_TABLE, props);
      ((TransactionAware) myTable2).startTx(tx2);

      TableAssert.assertRow(a(), myTable2.get(R1, a(C1, C2, C5)));
      Assert.assertArrayEquals(null, myTable2.get(R1, C1));
      Assert.assertArrayEquals(null, myTable2.get(R1, C2));
      Assert.assertArrayEquals(null, myTable2.get(R1, C5));
      TableAssert.assertRow(a(), myTable2.get(R1));
      doIncrement(doIncrAndGet, myTable2, R1, a(C1), la(55L), lb(55L));

      // start tx3 and TableAssert.verify same thing again
      Transaction tx3 = txClient.startShort();
      myTable3 = getTable(CONTEXT1, MY_TABLE, props);
      ((TransactionAware) myTable3).startTx(tx3);
      TableAssert.assertRow(a(), myTable3.get(R1, a(C1, C2, C5)));
      Assert.assertArrayEquals(null, myTable3.get(R1, C1));
      Assert.assertArrayEquals(null, myTable3.get(R1, C2));
      Assert.assertArrayEquals(null, myTable3.get(R1, C5));
      TableAssert.assertRow(a(), myTable3.get(R1));
      doIncrement(doIncrAndGet, myTable3, R1, a(C1), la(4L), lb(4L));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // TableAssert.verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // start tx4 and TableAssert.verify that changes of tx1 are now visible
      Transaction tx4 = txClient.startShort();
      myTable4 = getTable(CONTEXT1, MY_TABLE, props);
      ((TransactionAware) myTable4).startTx(tx4);
      TableAssert.assertRow(a(C1, L1, C2, L2, C5, V5), myTable4.get(R1, a(C1, C2, C3, C4, C5)));
      TableAssert.assertRow(a(C2, L2), myTable4.get(R1, a(C2)));
      Assert.assertArrayEquals(L1, myTable4.get(R1, C1));
      Assert.assertArrayEquals(L2, myTable4.get(R1, C2));
      Assert.assertArrayEquals(null, myTable4.get(R1, C3));
      Assert.assertArrayEquals(V5, myTable4.get(R1, C5));
      TableAssert.assertRow(a(C1, L1, C5, V5), myTable4.get(R1, a(C1, C5)));
      TableAssert.assertRow(a(C1, L1, C2, L2, C5, V5), myTable4.get(R1));

      // tx3 still cannot see tx1 changes, only its own
      TableAssert.assertRow(a(C1, L4), myTable3.get(R1, a(C1, C2, C5)));
      Assert.assertArrayEquals(L4, myTable3.get(R1, C1));
      Assert.assertArrayEquals(null, myTable3.get(R1, C2));
      Assert.assertArrayEquals(null, myTable3.get(R1, C5));
      TableAssert.assertRow(a(C1, L4), myTable3.get(R1));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      ((TransactionAware) myTable3).rollbackTx();
      txClient.abort(tx3);

      // TableAssert.verify we can do some ops with tx4 based on data written with tx1
      doIncrement(doIncrAndGet, myTable4, R1, a(C1, C2, C3), la(2L, 1L, 5L), lb(3L, 3L, 5L));
      myTable4.delete(R1, a(C2));
      doIncrement(doIncrAndGet, myTable4, R1, a(C4), la(3L), lb(3L));
      myTable4.delete(R1, a(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // TableAssert.verifying the result contents in next transaction
      Transaction tx5 = txClient.startShort();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      TableAssert.assertRow(a(C3, L5, C4, L3, C5, V5), myTable4.get(R1, a(C1, C2, C3, C4, C5)));
      Assert.assertArrayEquals(null, myTable4.get(R1, C1));
      Assert.assertArrayEquals(null, myTable4.get(R1, C2));
      Assert.assertArrayEquals(L5, myTable4.get(R1, C3));
      Assert.assertArrayEquals(L3, myTable4.get(R1, C4));
      Assert.assertArrayEquals(V5, myTable4.get(R1, C5));
      TableAssert.assertRow(a(C3, L5, C4, L3, C5, V5), myTable4.get(R1));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  private void doIncrement(boolean doGet, Table t,
                           byte[] row, byte[][] cols, long[] increments, byte[][] expectedValues) {
    if (doGet) {
      TableAssert.assertColumns(cols, expectedValues, t.incrementAndGet(row, cols, increments));
    } else {
      t.increment(row, cols, increments);
    }
  }

  @Test
  public void testBasicIncrementWriteWithTxSmall() throws Exception {
    testBasicIncrementWriteWithTxSmall(false);
    if (isReadlessIncrementSupported()) {
      testBasicIncrementWriteWithTxSmall(true);
    }
  }

  private void testBasicIncrementWriteWithTxSmall(boolean readless) throws Exception {
    DatasetProperties props = DatasetProperties.builder().add(
      Table.PROPERTY_READLESS_INCREMENT, String.valueOf(readless)).build();
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE, props);
    admin.create();
    Table myTable = getTable(CONTEXT1, MY_TABLE, props);

    // start 1st tx
    Transaction tx = txClient.startShort();
    ((TransactionAware) myTable).startTx(tx);

    myTable.increment(R1, a(C1), la(-3L));
    // we'll use this one to test that we can delete increment and increment again
    myTable.increment(R2, a(C2), la(5L));

    commitAndAssertSuccess(tx, (TransactionAware) myTable);

    // start 2nd tx
    tx = txClient.startShort();
    ((TransactionAware) myTable).startTx(tx);

    Assert.assertArrayEquals(Bytes.toBytes(-3L), myTable.get(R1, C1));
    myTable.increment(R1, a(C1), la(-3L));
    Assert.assertArrayEquals(Bytes.toBytes(-6L), myTable.get(R1, C1));

    Assert.assertArrayEquals(Bytes.toBytes(5L), myTable.get(R2, C2));
    myTable.delete(R2, C2);
    Assert.assertArrayEquals(null, myTable.get(R2, C2));

    commitAndAssertSuccess(tx, (TransactionAware) myTable);

    // start 3rd tx
    tx = txClient.startShort();
    ((TransactionAware) myTable).startTx(tx);

    Assert.assertArrayEquals(Bytes.toBytes(-6L), myTable.get(R1, C1));

    Assert.assertArrayEquals(null, myTable.get(R2, C2));
    myTable.increment(R2, a(C2), la(7L));
    Assert.assertArrayEquals(Bytes.toBytes(7L), myTable.get(R2, C2));

    commitAndAssertSuccess(tx, (TransactionAware) myTable);

    // start 4rd tx
    tx = txClient.startShort();
    ((TransactionAware) myTable).startTx(tx);

    Assert.assertArrayEquals(Bytes.toBytes(7L), myTable.get(R2, C2));

    commitAndAssertSuccess(tx, (TransactionAware) myTable);

    admin.drop();
  }

  private void commitAndAssertSuccess(Transaction tx, TransactionAware txAware) throws Exception {
    Assert.assertTrue(txClient.canCommit(tx, txAware.getTxChanges()));
    Assert.assertTrue(txAware.commitTx());
    Assert.assertTrue(txClient.commit(tx));
  }

  @Test
  public void testBasicDeleteWithTx() throws Exception {
    // we will test 3 different delete column ops and one delete row op
    // * delete column with delete
    // * delete column with put null value
    // * delete column with put byte[0] value
    // * delete row with delete
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      // write smth and commit
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1, C2), a(V1, V2));
      myTable1.put(R2, a(C1, C2), a(V2, V3));
      myTable1.put(R3, a(C1, C2), a(V3, V4));
      myTable1.put(R4, a(C1, C2), a(V4, V5));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test delete ops
      // start new tx2
      Transaction tx2 = txClient.startShort();
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      // TableAssert.verify tx2 sees changes of tx1
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable2.get(R1, a(C1, C2)));
      // TableAssert.verify tx2 sees changes of tx1
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable2.get(R2));
      // delete c1, r2
      myTable2.delete(R1, a(C1));
      myTable2.delete(R2);
      // same as delete a column
      myTable2.put(R3, C1, null);
      // TableAssert.verify can see deletes in own changes before commit
      TableAssert.assertRow(a(C2, V2), myTable2.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(null, myTable2.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable2.get(R1, C2));
      TableAssert.assertRow(a(), myTable2.get(R2));
      TableAssert.assertRow(a(C2, V4), myTable2.get(R3));
      // overwrite c2 and write new value to c1
      myTable2.put(R1, a(C1, C2), a(V3, V4));
      myTable2.put(R2, a(C1, C2), a(V4, V5));
      myTable2.put(R3, a(C1, C2), a(V1, V2));
      myTable2.put(R4, a(C1, C2), a(V2, V3));
      // TableAssert.verify can see changes in own changes before commit
      TableAssert.assertRow(a(C1, V3, C2, V4), myTable2.get(R1, a(C1, C2, C3)));
      Assert.assertArrayEquals(V3, myTable2.get(R1, C1));
      Assert.assertArrayEquals(V4, myTable2.get(R1, C2));
      Assert.assertArrayEquals(null, myTable2.get(R1, C3));
      TableAssert.assertRow(a(C1, V4, C2, V5), myTable2.get(R2));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable2.get(R3));
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable2.get(R4));
      // delete c2 and r2
      myTable2.delete(R1, a(C2));
      myTable2.delete(R2);
      myTable2.put(R1, C2, null);
      // TableAssert.verify that delete is there (i.e. not reverted to whatever was persisted before)
      TableAssert.assertRow(a(C1, V3), myTable2.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V3, myTable2.get(R1, C1));
      Assert.assertArrayEquals(null, myTable2.get(R1, C2));
      TableAssert.assertRow(a(), myTable2.get(R2));
      Assert.assertArrayEquals(V1, myTable2.get(R3, C1));
      Assert.assertArrayEquals(V2, myTable2.get(R4, C1));

      // start tx3 and TableAssert.verify that changes of tx2 are not visible yet
      Transaction tx3 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx3);
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1));
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable1.get(R2));
      TableAssert.assertRow(a(C1, V3, C2, V4), myTable1.get(R3));
      TableAssert.assertRow(a(C1, V4, C2, V5), myTable1.get(R4));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // starting tx4 before committing tx2 so that we can check conflicts are detected wrt deletes
      Transaction tx4 = txClient.startShort();
      // starting tx5 before committing tx2 so that we can check conflicts are detected wrt deletes
      Transaction tx5 = txClient.startShort();

      // commit tx2 in stages to see how races are handled wrt delete ops
      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());

      // start tx6 and TableAssert.verify that changes of tx2 are not visible yet (even though they are flushed)
      Transaction tx6 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx6);
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1));
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable1.get(R2));
      TableAssert.assertRow(a(C1, V3, C2, V4), myTable1.get(R3));
      TableAssert.assertRow(a(C1, V4, C2, V5), myTable1.get(R4));

      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx6));

      // make tx2 visible
      Assert.assertTrue(txClient.commit(tx2));

      // start tx7 and TableAssert.verify that changes of tx2 are now visible
      Transaction tx7 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx7);
      TableAssert.assertRow(a(C1, V3), myTable1.get(R1, a(C1, C2)));
      TableAssert.assertRow(a(C1, V3), myTable1.get(R1));
      Assert.assertArrayEquals(V3, myTable1.get(R1, C1));
      Assert.assertArrayEquals(null, myTable1.get(R1, C2));
      TableAssert.assertRow(a(C1, V3), myTable1.get(R1, a(C1, C2)));
      TableAssert.assertRow(a(), myTable1.get(R2));
      Assert.assertArrayEquals(V1, myTable1.get(R3, C1));
      Assert.assertArrayEquals(V2, myTable1.get(R4, C1));

      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx7));

      // but not visible to tx4 that we started earlier than tx2 became visible
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx4);
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1));
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable1.get(R2));
      TableAssert.assertRow(a(C1, V3, C2, V4), myTable1.get(R3));
      TableAssert.assertRow(a(C1, V4, C2, V5), myTable1.get(R4));

      // writing to deleted column, to check conflicts are detected (delete-write conflict)
      myTable1.put(R1, a(C2), a(V5));
      Assert.assertFalse(txClient.canCommit(tx4, ((TransactionAware) myTable1).getTxChanges()));
      ((TransactionAware) myTable1).rollbackTx();
      txClient.abort(tx4);

      // deleting changed column, to check conflicts are detected (write-delete conflict)
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx5);
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      Assert.assertArrayEquals(V1, myTable1.get(R1, C1));
      Assert.assertArrayEquals(V2, myTable1.get(R1, C2));
      Assert.assertArrayEquals(null, myTable1.get(R1, C3));
      TableAssert.assertRow(a(C1, V1, C2, V2), myTable1.get(R1));
      TableAssert.assertRow(a(C1, V2, C2, V3), myTable1.get(R2));
      TableAssert.assertRow(a(C1, V3, C2, V4), myTable1.get(R3));
      TableAssert.assertRow(a(C1, V4, C2, V5), myTable1.get(R4));
      // NOTE: we are TableAssert.verifying conflict in one operation only. We may want to test each...
      myTable1.delete(R1, a(C1));
      Assert.assertFalse(txClient.canCommit(tx5, ((TransactionAware) myTable1).getTxChanges()));
      ((TransactionAware) myTable1).rollbackTx();
      txClient.abort(tx5);

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicScanWithTx() throws Exception {
    // todo: make work with tx well (merge with buffer, conflicts) and add tests for that
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      // write r1...r5 and commit
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1), a(V1));
      myTable1.put(R2, a(C2), a(V2));
      myTable1.put(R3, a(C3, C4), a(V3, V4));
      myTable1.put(R4, a(C4), a(V4));
      myTable1.put(R5, a(C5), a(V5));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test scans
      // currently not testing races/conflicts/etc as this logic is not there for scans yet; so using one same tx
      Transaction tx2 = txClient.startShort();
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded scan
      TableAssert.assertScan(a(R2, R3, R4),
                             aa(a(C2, V2),
                                a(C3, V3, C4, V4),
                                a(C4, V4)),
                             myTable2, new Scan(R2, R5));
      // open start scan
      TableAssert.assertScan(a(R1, R2, R3),
                             aa(a(C1, V1),
                                a(C2, V2),
                                a(C3, V3, C4, V4)),
                             myTable2, new Scan(null, R4));
      // open end scan
      TableAssert.assertScan(a(R3, R4, R5),
                             aa(a(C3, V3, C4, V4),
                                a(C4, V4),
                                a(C5, V5)),
                             myTable2, new Scan(R3, null));
      // open ends scan
      TableAssert.assertScan(a(R1, R2, R3, R4, R5),
                             aa(a(C1, V1),
                                a(C2, V2),
                                a(C3, V3, C4, V4),
                                a(C4, V4),
                                a(C5, V5)),
                             myTable2, new Scan(null, null));

      // adding/changing/removing some columns
      myTable2.put(R2, a(C1, C2, C3), a(V4, V3, V2));
      myTable2.delete(R3, a(C4));

      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());
      Assert.assertTrue(txClient.commit(tx2));

      // Checking that changes are reflected in new scans in new tx
      Transaction tx3 = txClient.startShort();
      // NOTE: table can be re-used betweet tx
      ((TransactionAware) myTable1).startTx(tx3);

      TableAssert.assertScan(a(R2, R3, R4),
                             aa(a(C1, V4, C2, V3, C3, V2),
                                a(C3, V3),
                                a(C4, V4)),
                             myTable1, new Scan(R2, R5));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testMultiGetWithTx() throws Exception {
    String testMultiGet = "testMultiGet";
    DatasetAdmin admin = getTableAdmin(CONTEXT1, testMultiGet);
    admin.create();
    try {
      Transaction tx = txClient.startShort();
      Table table = getTable(CONTEXT1, testMultiGet);
      ((TransactionAware) table).startTx(tx);
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes("r" + i)).add(C1, V1).add(C2, V2));
      }
      Assert.assertTrue(txClient.canCommit(tx, ((TransactionAware) table).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table).commitTx());
      Assert.assertTrue(txClient.commit(tx));

      Transaction tx2 = txClient.startShort();
      ((TransactionAware) table).startTx(tx2);
      List<Get> gets = Lists.newArrayListWithCapacity(100);
      for (int i = 0; i < 100; i++) {
        gets.add(new Get(Bytes.toBytes("r" + i)));
      }
      List<Row> results = table.get(gets);
      Assert.assertTrue(txClient.commit(tx2));
      for (int i = 0; i < 100; i++) {
        Row row = results.get(i);
        Assert.assertArrayEquals(Bytes.toBytes("r" + i), row.getRow());
        byte[] val = row.get(C1);
        Assert.assertNotNull(val);
        Assert.assertArrayEquals(V1, val);
        byte[] val2 = row.get(C2);
        Assert.assertNotNull(val2);
        Assert.assertArrayEquals(V2, val2);
      }

      Transaction tx3 = txClient.startShort();
      ((TransactionAware) table).startTx(tx3);
      gets = Lists.newArrayListWithCapacity(100);
      for (int i = 0; i < 100; i++) {
        gets.add(new Get("r" + i).add(C1));
      }
      results = table.get(gets);
      Assert.assertTrue(txClient.commit(tx3));
      for (int i = 0; i < 100; i++) {
        Row row = results.get(i);
        Assert.assertArrayEquals(Bytes.toBytes("r" + i), row.getRow());
        byte[] val = row.get(C1);
        Assert.assertNotNull(val);
        Assert.assertArrayEquals(V1, val);
        // should have only returned column 1
        byte[] val2 = row.get(C2);
        Assert.assertNull(val2);
      }

      // retrieve different columns per row
      Transaction tx4 = txClient.startShort();
      ((TransactionAware) table).startTx(tx4);
      gets = Lists.newArrayListWithCapacity(100);
      for (int i = 0; i < 100; i++) {
        Get get = new Get("r" + i);
        // evens get C1, odds get C2
        get.add(i % 2 == 0 ? C1 : C2);
        gets.add(get);
      }
      results = table.get(gets);
      Assert.assertTrue(txClient.commit(tx4));
      for (int i = 0; i < 100; i++) {
        Row row = results.get(i);
        Assert.assertArrayEquals(Bytes.toBytes("r" + i), row.getRow());
        byte[] val1 = row.get(C1);
        byte[] val2 = row.get(C2);
        if (i % 2 == 0) {
          Assert.assertNotNull(val1);
          Assert.assertArrayEquals(V1, val1);
          Assert.assertNull(val2);
        } else {
          Assert.assertNull(val1);
          Assert.assertNotNull(val2);
          Assert.assertArrayEquals(V2, val2);
        }
      }
    } finally {
      admin.drop();
    }
  }

  @Test
  public void testScanAndDelete() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      //
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);

      myTable1.put(Bytes.toBytes("1_09a"), a(C1), a(V1));

      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      //
      Transaction tx2 = txClient.startShort();
      ((TransactionAware) myTable1).startTx(tx2);

      myTable1.delete(Bytes.toBytes("1_09a"));
      myTable1.put(Bytes.toBytes("1_08a"), a(C1), a(V1));

      myTable1.put(Bytes.toBytes("1_09b"), a(C1), a(V1));

      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx2));

      //
      Transaction tx3 = txClient.startShort();
      ((TransactionAware) myTable1).startTx(tx3);

      TableAssert.assertScan(a(Bytes.toBytes("1_08a"), Bytes.toBytes("1_09b")),
                             aa(a(C1, V1),
                                a(C1, V1)),
                             myTable1, new Scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

      myTable1.delete(Bytes.toBytes("1_08a"));
      myTable1.put(Bytes.toBytes("1_07a"), a(C1), a(V1));

      myTable1.delete(Bytes.toBytes("1_09b"));
      myTable1.put(Bytes.toBytes("1_08b"), a(C1), a(V1));

      myTable1.put(Bytes.toBytes("1_09c"), a(C1), a(V1));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // Now, we will test scans
      Transaction tx4 = txClient.startShort();
      ((TransactionAware) myTable1).startTx(tx4);

      TableAssert.assertScan(a(Bytes.toBytes("1_07a"), Bytes.toBytes("1_08b"), Bytes.toBytes("1_09c")),
                             aa(a(C1, V1),
                                a(C1, V1),
                                a(C1, V1)),
                             myTable1, new Scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testScanWithFuzzyRowFilter() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      Table table = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) table).startTx(tx1);

      // write data
      byte[] abc = { 'a', 'b', 'c' };
      for (byte b1 : abc) {
        for (byte b2 : abc) {
          for (byte b3 : abc) {
            for (byte b4 : abc) {
              table.put(new Put(new byte[] { b1, b2, b3, b4 }).add(C1, V1));
            }
          }
        }
      }

      // we should have 81 (3^4) rows now
      Assert.assertEquals(81, countRows(table));

      // check that filter works against data written in same tx
      verifyScanWithFuzzyRowFilter(table);

      // commit tx, start new and TableAssert.verify scan again against "persisted" data
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) table).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table).commitTx());
      Assert.assertTrue(txClient.commit(tx1));
      ((TransactionAware) table).postTxCommit();

      Transaction tx2 = txClient.startShort();
      ((TransactionAware) table).startTx(tx2);
      verifyScanWithFuzzyRowFilter(table);

    } finally {
      admin.drop();
    }
  }

  private static void verifyScanWithFuzzyRowFilter(Table table) {
    FuzzyRowFilter filter = new FuzzyRowFilter(
      ImmutableList.of(ImmutablePair.of(new byte[]{'*', 'b', '*', 'b'}, new byte[]{0x01, 0x00, 0x01, 0x00})));
    Scanner scanner = table.scan(new Scan(null, null, filter));
    int count = 0;
    while (true) {
      Row entry = scanner.next();
      if (entry == null) {
        break;
      }
      Assert.assertTrue(entry.getRow()[1] == 'b' && entry.getRow()[3] == 'b');
      Assert.assertEquals(1, entry.getColumns().size());
      Assert.assertTrue(entry.getColumns().containsKey(C1));
      Assert.assertArrayEquals(V1, entry.get(C1));
      count++;
    }
    Assert.assertEquals(9, count);
  }

  private static int countRows(Table table) throws Exception {
    Scanner scanner = table.scan(null, null);
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    return count;
  }


  @Test
  public void testBasicColumnRangeWithTx() throws Exception {
    // todo: test more tx logic (or add to get/put unit-test)
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      // write test data and commit
      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1, C2, C3, C4, C5), a(V1, V2, V3, V4, V5));
      myTable1.put(R2, a(C1), a(V2));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test column range get
      Transaction tx2 = txClient.startShort();
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded range
      TableAssert.assertRow(a(C2, V2, C3, V3, C4, V4),
                            myTable2.get(R1, C2, C5, Integer.MAX_VALUE));
      // open start range
      TableAssert.assertRow(a(C1, V1, C2, V2, C3, V3),
                            myTable2.get(R1, null, C4, Integer.MAX_VALUE));
      // open end range
      TableAssert.assertRow(a(C3, V3, C4, V4, C5, V5),
                            myTable2.get(R1, C3, null, Integer.MAX_VALUE));
      // open ends range
      TableAssert.assertRow(a(C1, V1, C2, V2, C3, V3, C4, V4, C5, V5),
                            myTable2.get(R1, null, null, Integer.MAX_VALUE));

      // same with limit
      // bounded range with limit
      TableAssert.assertRow(a(C2, V2),
                            myTable2.get(R1, C2, C5, 1));
      // open start range with limit
      TableAssert.assertRow(a(C1, V1, C2, V2),
                            myTable2.get(R1, null, C4, 2));
      // open end range with limit
      TableAssert.assertRow(a(C3, V3, C4, V4),
                            myTable2.get(R1, C3, null, 2));
      // open ends range with limit
      TableAssert.assertRow(a(C1, V1, C2, V2, C3, V3, C4, V4),
                            myTable2.get(R1, null, null, 4));

      // adding/changing/removing some columns
      myTable2.put(R1, a(C1, C2, C3), a(V4, V3, V2));
      myTable2.delete(R1, a(C4));

      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());
      Assert.assertTrue(txClient.commit(tx2));

      // Checking that changes are reflected in new scans in new tx
      Transaction tx3 = txClient.startShort();
      // NOTE: table can be re-used betweet tx
      ((TransactionAware) myTable1).startTx(tx3);

      TableAssert.assertRow(a(C2, V3, C3, V2),
                            myTable1.get(R1, C2, C5, Integer.MAX_VALUE));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBatchWritableKeyIsIgnored() throws Exception {
    String tableName = "batchWritableTable";
    getTableAdmin(CONTEXT1, tableName).create();
    try {
      // write in a transaction, three times, with key = null, a, q, always Put with row = a
      Transaction tx = txClient.startShort();
      Table table = getTable(CONTEXT1, tableName);
      ((TransactionAware) table).startTx(tx);
      table.write(null, new Put("a").add("x", "x"));
      table.write(new byte[]{'q'}, new Put("a").add("y", "y"));
      table.write(new byte[]{'a'}, new Put("a").add("z", "z"));
      Assert.assertTrue(txClient.canCommit(tx, ((TransactionAware) table).getTxChanges()));
      ((TransactionAware) table).commitTx();
      Assert.assertTrue(txClient.commit(tx));

      // validate that all writes went to row a, and row q was not written
      tx = txClient.startShort();
      ((TransactionAware) table).startTx(tx);
      Assert.assertTrue(table.get(new Get("q")).isEmpty());
      Row row = table.get(new Get("a"));
      Assert.assertEquals(3, row.getColumns().size());
      Assert.assertEquals("x", row.getString("x"));
      Assert.assertEquals("y", row.getString("y"));
      Assert.assertEquals("z", row.getString("z"));
      ((TransactionAware) table).commitTx();
      txClient.abort(tx);

    } finally {
      getTableAdmin(CONTEXT1, tableName).drop();
    }
  }

  @Test
  public void testTxUsingMultipleTables() throws Exception {
    String table1 = "table1";
    String table2 = "table2";
    String table3 = "table3";
    String table4 = "table4";
    getTableAdmin(CONTEXT1, table1).create();
    getTableAdmin(CONTEXT1, table2).create();
    getTableAdmin(CONTEXT1, table3).create();
    getTableAdmin(CONTEXT1, table4).create();

    try {
      // We will be changing:
      // * table1 and table2 in tx1
      // * table2 and table3 in tx2 << will conflict with first one
      // * table3 and table4 in tx3

      Transaction tx1 = txClient.startShort();
      Transaction tx2 = txClient.startShort();
      Transaction tx3 = txClient.startShort();

      // Write data in tx1 and commit
      Table table11 = getTable(CONTEXT1, table1);
      ((TransactionAware) table11).startTx(tx1);
      // write r1->c1,v1 but not commit
      table11.put(R1, a(C1), a(V1));
      Table table21 = getTable(CONTEXT1, table2);
      ((TransactionAware) table21).startTx(tx1);
      // write r1->c1,v2 but not commit
      table21.put(R1, a(C1), a(V2));
      // TableAssert.verify writes inside same tx
      TableAssert.assertRow(a(C1, V1), table11.get(R1, a(C1)));
      TableAssert.assertRow(a(C1, V2), table21.get(R1, a(C1)));
      // commit tx1
      Assert.assertTrue(txClient.canCommit(tx1, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table11).getTxChanges(),
                         ((TransactionAware) table21).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table11).commitTx());
      Assert.assertTrue(((TransactionAware) table21).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Write data in tx2 and check that cannot commit because of conflicts
      Table table22 = getTable(CONTEXT1, table2);
      ((TransactionAware) table22).startTx(tx2);
      // write r1->c1,v1 but not commit
      table22.put(R1, a(C1), a(V2));
      Table table32 = getTable(CONTEXT1, table3);
      ((TransactionAware) table32).startTx(tx2);
      // write r1->c1,v2 but not commit
      table32.put(R1, a(C1), a(V3));
      // TableAssert.verify writes inside same tx
      TableAssert.assertRow(a(C1, V2), table22.get(R1, a(C1)));
      TableAssert.assertRow(a(C1, V3), table32.get(R1, a(C1)));
      // try commit tx2
      Assert.assertFalse(txClient.canCommit(tx2, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table22).getTxChanges(),
                         ((TransactionAware) table32).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table22).rollbackTx());
      Assert.assertTrue(((TransactionAware) table32).rollbackTx());
      txClient.abort(tx2);

      // Write data in tx3 and check that can commit (no conflicts)
      Table table33 = getTable(CONTEXT1, table3);
      ((TransactionAware) table33).startTx(tx3);
      // write r1->c1,v1 but not commit
      table33.put(R1, a(C1), a(V3));
      Table table43 = getTable(CONTEXT1, table4);
      ((TransactionAware) table43).startTx(tx3);
      // write r1->c1,v2 but not commit
      table43.put(R1, a(C1), a(V4));
      // TableAssert.verify writes inside same tx
      TableAssert.assertRow(a(C1, V3), table33.get(R1, a(C1)));
      TableAssert.assertRow(a(C1, V4), table43.get(R1, a(C1)));
      // commit tx3
      Assert.assertTrue(txClient.canCommit(tx3, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table33).getTxChanges(),
                         ((TransactionAware) table43).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table33).commitTx());
      Assert.assertTrue(((TransactionAware) table43).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      getTableAdmin(CONTEXT1, table1).drop();
      getTableAdmin(CONTEXT1, table2).drop();
      getTableAdmin(CONTEXT1, table3).drop();
      getTableAdmin(CONTEXT1, table4).drop();
    }
  }

  @Test
  public void testConflictsNoneLevel() throws Exception {
    testConflictDetection(ConflictDetection.NONE);
  }

  @Test
  public void testConflictsOnRowLevel() throws Exception {
    testConflictDetection(ConflictDetection.ROW);
  }

  @Test
  public void testConflictsOnColumnLevel() throws Exception {
    testConflictDetection(ConflictDetection.COLUMN);
  }

  private void testConflictDetection(ConflictDetection level) throws Exception {
    // we use tableX_Y format for variable names which means "tableX that is used in tx Y"

    String table1 = "table1";
    String table2 = "table2";
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_CONFLICT_LEVEL, level.name()).build();
    DatasetAdmin admin1 = getTableAdmin(CONTEXT1, table1, props);
    DatasetAdmin admin2 = getTableAdmin(CONTEXT1, table2, props);
    admin1.create();
    admin2.create();
    try {
      // 1) Test conflicts when using different tables

      Transaction tx1 = txClient.startShort();
      Table table11 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table11).startTx(tx1);
      // write r1->c1,v1 but not commit
      table11.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx2 = txClient.startShort();
      Table table22 = getTable(CONTEXT1, table2, props);
      ((TransactionAware) table22).startTx(tx2);

      // change in tx2 same data but in different table
      table22.put(R1, a(C1), a(V2));

      // start new tx
      Transaction tx3 = txClient.startShort();
      Table table13 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table13).startTx(tx3);

      // change in tx3 same data in same table as tx1
      table13.put(R1, a(C1), a(V2));

      // committing tx1
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) table11).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table11).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // no conflict should be when committing tx2
      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) table22).getTxChanges()));

      // but conflict should be when committing tx3
      if (level != ConflictDetection.NONE) {
        Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) table13).getTxChanges()));
        ((TransactionAware) table13).rollbackTx();
        txClient.abort(tx3);
      } else {
        Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) table13).getTxChanges()));
      }

      // 2) Test conflicts when using different rows
      Transaction tx4 = txClient.startShort();
      Table table14 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table14).startTx(tx4);
      // write r1->c1,v1 but not commit
      table14.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx5 = txClient.startShort();
      Table table15 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table15).startTx(tx5);

      // change in tx5 same data but in different row
      table15.put(R2, a(C1), a(V2));

      // start new tx
      Transaction tx6 = txClient.startShort();
      Table table16 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table16).startTx(tx6);

      // change in tx6 in same row as tx1
      table16.put(R1, a(C2), a(V2));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) table14).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table14).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // no conflict should be when committing tx5
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) table15).getTxChanges()));

      // but conflict should be when committing tx6 iff we resolve on row level
      if (level == ConflictDetection.ROW) {
        Assert.assertFalse(txClient.canCommit(tx6, ((TransactionAware) table16).getTxChanges()));
        ((TransactionAware) table16).rollbackTx();
        txClient.abort(tx6);
      } else {
        Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) table16).getTxChanges()));
      }

      // 3) Test conflicts when using different columns
      Transaction tx7 = txClient.startShort();
      Table table17 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table17).startTx(tx7);
      // write r1->c1,v1 but not commit
      table17.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx8 = txClient.startShort();
      Table table18 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table18).startTx(tx8);

      // change in tx8 same data but in different column
      table18.put(R1, a(C2), a(V2));

      // start new tx
      Transaction tx9 = txClient.startShort();
      Table table19 = getTable(CONTEXT1, table1, props);
      ((TransactionAware) table19).startTx(tx9);

      // change in tx9 same column in same column as tx1
      table19.put(R1, a(C1), a(V2));

      // committing tx7
      Assert.assertTrue(txClient.canCommit(tx7, ((TransactionAware) table17).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table17).commitTx());
      Assert.assertTrue(txClient.commit(tx7));

      // no conflict should be when committing tx8 iff we resolve on column level
      if (level == ConflictDetection.COLUMN || level == ConflictDetection.NONE) {
        Assert.assertTrue(txClient.canCommit(tx8, ((TransactionAware) table18).getTxChanges()));
      } else {
        Assert.assertFalse(txClient.canCommit(tx8, ((TransactionAware) table18).getTxChanges()));
        ((TransactionAware) table18).rollbackTx();
        txClient.abort(tx8);
      }

      // but conflict should be when committing tx9
      if (level != ConflictDetection.NONE) {
        Assert.assertFalse(txClient.canCommit(tx9, ((TransactionAware) table19).getTxChanges()));
        ((TransactionAware) table19).rollbackTx();
        txClient.abort(tx9);
      } else {
        Assert.assertTrue(txClient.canCommit(tx9, ((TransactionAware) table19).getTxChanges()));
      }

    } finally {
      // NOTE: we are doing our best to cleanup junk between tests to isolate errors, but we are not going to be
      //       crazy about it
      admin1.drop();
      admin2.drop();
    }
  }

  @Test
  public void testRollingBackPersistedChanges() throws Exception {
    DatasetAdmin admin = getTableAdmin(CONTEXT1, MY_TABLE);
    admin.create();
    try {
      // write and commit one row/column
      Transaction tx0 = txClient.startShort();
      Table myTable0 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable0).startTx(tx0);
      myTable0.put(R2, a(C2), a(V2));
      Assert.assertTrue(txClient.canCommit(tx0, ((TransactionAware) myTable0).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable0).commitTx());
      Assert.assertTrue(txClient.commit(tx0));
      ((TransactionAware) myTable0).postTxCommit();

      Transaction tx1 = txClient.startShort();
      Table myTable1 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // also overwrite the value from tx0
      myTable1.put(R2, a(C2), a(V3));
      // TableAssert.verify can see changes inside tx
      TableAssert.assertRow(a(C1, V1), myTable1.get(R1, a(C1)));

      // persisting changes
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // let's pretend that after persisting changes we still got conflict when finalizing tx, so
      // rolling back changes
      Assert.assertTrue(((TransactionAware) myTable1).rollbackTx());

      // making tx visible
      txClient.abort(tx1);

      // start new tx
      Transaction tx2 = txClient.startShort();
      Table myTable2 = getTable(CONTEXT1, MY_TABLE);
      ((TransactionAware) myTable2).startTx(tx2);

      // TableAssert.verify don't see rolled back changes
      TableAssert.assertRow(a(), myTable2.get(R1, a(C1)));
      // TableAssert.verify we still see the previous value
      TableAssert.assertRow(a(C2, V2), myTable2.get(R2, a(C2)));

    } finally {
      admin.drop();
    }
  }

  // this test ensures that an existing client survives the truncating or dropping and recreating of a table
  @Test
  public void testClientSurvivesTableReset() throws Exception {
    final String tableName = "survive";
    DatasetAdmin admin = getTableAdmin(CONTEXT1, tableName);
    admin.create();
    Table table = getTable(CONTEXT1, tableName);

    // write some values
    Transaction tx0 = txClient.startShort();
    ((TransactionAware) table).startTx(tx0);
    table.put(R1, a(C1), a(V1));
    Assert.assertTrue(txClient.canCommit(tx0, ((TransactionAware) table).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table).commitTx());
    Assert.assertTrue(txClient.commit(tx0));
    ((TransactionAware) table).postTxCommit();

    // TableAssert.verify
    Transaction tx1 = txClient.startShort();
    ((TransactionAware) table).startTx(tx1);
    TableAssert.assertRow(a(C1, V1), table.get(R1));

    // drop table and recreate
    admin.drop();
    admin.create();

    // TableAssert.verify can read but nothing there
    TableAssert.assertRow(a(), table.get(R1));
    txClient.abort(tx1); // only did read, safe to abort

    // create a new client and write another value
    Table table2 = getTable(CONTEXT1, tableName);
    Transaction tx2 = txClient.startShort();
    ((TransactionAware) table2).startTx(tx2);
    table2.put(R1, a(C2), a(V2));
    Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) table2).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table2).commitTx());
    Assert.assertTrue(txClient.commit(tx2));
    ((TransactionAware) table2).postTxCommit();

    // TableAssert.verify it is visible
    Transaction tx3 = txClient.startShort();
    ((TransactionAware) table).startTx(tx3);
    TableAssert.assertRow(a(C2, V2), table.get(R1));

    // truncate table
    admin.truncate();

    // TableAssert.verify can read but nothing there
    TableAssert.assertRow(a(), table.get(R1));
    txClient.abort(tx3); // only did read, safe to abort

    // write again with other client
    Transaction tx4 = txClient.startShort();
    ((TransactionAware) table2).startTx(tx4);
    table2.put(R1, a(C3), a(V3));
    Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) table2).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table2).commitTx());
    Assert.assertTrue(txClient.commit(tx4));
    ((TransactionAware) table2).postTxCommit();

    // TableAssert.verify it is visible
    Transaction tx5 = txClient.startShort();
    ((TransactionAware) table).startTx(tx5);
    TableAssert.assertRow(a(C3, V3), table.get(R1));
    txClient.abort(tx5); // only did read, safe to abort

    // drop table
    admin.drop();
  }

  @Test
  public void testMetrics() throws Exception {
    testMetrics(false);
    if (isReadlessIncrementSupported()) {
      testMetrics(true);
    }
  }

  private void testMetrics(boolean readless) throws Exception {
    final String tableName = "survive";
    DatasetProperties props = DatasetProperties.builder().add(
      Table.PROPERTY_READLESS_INCREMENT, String.valueOf(readless)).build();
    DatasetAdmin admin = getTableAdmin(CONTEXT1, tableName, props);
    admin.create();
    Table table = getTable(CONTEXT1, tableName, props);
    final Map<String, Long> metrics = Maps.newHashMap();
    ((MeteredDataset) table).setMetricsCollector(new MetricsCollector() {
      @Override
      public void increment(String metricName, long value) {
        Long old = metrics.get(metricName);
        metrics.put(metricName, old == null ? value : old + value);
      }

      @Override
      public void gauge(String metricName, long value) {
        metrics.put(metricName, value);
      }
    });

    // Note that we don't need to finish tx for metrics to be reported
    Transaction tx0 = txClient.startShort();
    ((TransactionAware) table).startTx(tx0);

    int writes = 0;
    int reads = 0;

    table.put(new Put(R1, C1, V1));
    verifyDatasetMetrics(metrics, ++writes, reads);

    table.compareAndSwap(R1, C1, V1, V2);
    verifyDatasetMetrics(metrics, ++writes, ++reads);

    // note: will not write anything as expected value will not match
    table.compareAndSwap(R1, C1, V1, V2);
    verifyDatasetMetrics(metrics, writes, ++reads);

    table.increment(new Increment(R2, C2, 1L));
    if (readless) {
      verifyDatasetMetrics(metrics, ++writes, reads);
    } else {
      verifyDatasetMetrics(metrics, ++writes, ++reads);
    }

    table.incrementAndGet(new Increment(R2, C2, 1L));
    verifyDatasetMetrics(metrics, ++writes, ++reads);

    table.get(new Get(R1, C1, V1));
    verifyDatasetMetrics(metrics, writes, ++reads);

    Scanner scanner = table.scan(new Scan(null, null));
    while (scanner.next() != null) {
      verifyDatasetMetrics(metrics, writes, ++reads);
    }

    table.delete(new Delete(R1, C1, V1));
    verifyDatasetMetrics(metrics, ++writes, reads);

    // drop table
    admin.drop();
  }

  @Test
  public void testReadOwnWrite() throws Exception {
    final String tableName = "readOwnWrite";
    DatasetAdmin admin = getTableAdmin(CONTEXT1, tableName);
    admin.create();
    Table table = getTable(CONTEXT1, tableName);

    Transaction tx = txClient.startShort();
    try {
      ((TransactionAware) table).startTx(tx);

      // Write some data, then flush it by calling commitTx.
      table.put(new Put(R1, C1, V1));

      ((TransactionAware) table).commitTx();

      // Try to read the previous write.
      Assert.assertArrayEquals(V1, table.get(new Get(R1, C1)).get(C1));
    } finally {
      txClient.commit(tx);
    }

    // drop table
    admin.drop();
  }

  @Test
  public void testMultiIncrementWithFlush() throws Exception {
    testMultiIncrementWithFlush(false);
    if (isReadlessIncrementSupported()) {
      testMultiIncrementWithFlush(true);
    }
  }

  private void testMultiIncrementWithFlush(boolean readless) throws Exception {
    final String tableName = "incrFlush";
    DatasetProperties props = DatasetProperties.builder()
      .add(Table.PROPERTY_READLESS_INCREMENT, String.valueOf(readless)).build();
    DatasetAdmin admin = getTableAdmin(CONTEXT1, tableName, props);
    admin.create();
    Map<String, String> args = new HashMap<>();
    if (readless) {
      args.put(HBaseTable.SAFE_INCREMENTS, "true");
    }
    Table table = getTable(CONTEXT1, tableName, props, args);

    Transaction tx = txClient.startShort();
    try {
      ((TransactionAware) table).startTx(tx);

      // Write an increment, then flush it by calling commitTx.
      table.increment(new Increment(R1, C1, 10L));
      ((TransactionAware) table).commitTx();
    } finally {
      // invalidate the tx, leaving an excluded write in the table
      txClient.invalidate(tx.getTransactionId());
    }

    // validate the first write is not visible
    tx = txClient.startShort();
    try {
      ((TransactionAware) table).startTx(tx);
      Assert.assertEquals(null, table.get(new Get(R1, C1)).getLong(C1));
    } finally {
      txClient.commit(tx);
    }

    tx = txClient.startShort();
    try {
      ((TransactionAware) table).startTx(tx);

      // Write an increment, then flush it by calling commitTx.
      table.increment(new Increment(R1, C1, 1L));
      ((TransactionAware) table).commitTx();

      // Write another increment, from both table instances
      table.increment(new Increment(R1, C1, 1L));

      if (readless) {
        Table table2 = getTable(CONTEXT1, tableName, props, args);
        ((TransactionAware) table2).startTx(tx);
        table2.increment(new Increment(R1, C1, 1L));
        ((TransactionAware) table2).commitTx();
      }

      ((TransactionAware) table).commitTx();

    } finally {
      txClient.commit(tx);
    }

    // validate all increments are visible to a new tx
    tx = txClient.startShort();
    try {
      ((TransactionAware) table).startTx(tx);
      Assert.assertEquals(new Long(readless ? 3L : 2L), table.get(new Get(R1, C1)).getLong(C1));
    } finally {
      txClient.commit(tx);
    }

    // drop table
    admin.drop();
  }

  private void verifyDatasetMetrics(Map<String, Long> metrics, long writes, long reads) {
    Assert.assertEquals(writes, getLong(metrics, Constants.Metrics.Name.Dataset.WRITE_COUNT));
    Assert.assertEquals(reads, getLong(metrics, Constants.Metrics.Name.Dataset.READ_COUNT));
    Assert.assertEquals(writes + reads, getLong(metrics, Constants.Metrics.Name.Dataset.OP_COUNT));
  }

  private long getLong(Map<String, Long> map, String key) {
    Long value = map.get(key);
    return value == null ? 0 : value;
  }

  private static long[] la(long... elems) {
    return elems;
  }

  static byte[][] lb(long... elems) {
    byte[][] elemBytes = new byte[elems.length][];
    for (int i = 0; i < elems.length; i++) {
      elemBytes[i] = Bytes.toBytes(elems[i]);
    }
    return elemBytes;
  }

  // to array
  static byte[][] a(byte[]... elems) {
    return elems;
  }

  // to array of array
  static byte[][][] aa(byte[][]... elems) {
    return elems;
  }
}
