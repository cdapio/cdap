package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.table.Scanner;
import com.continuuity.data2.dataset.api.DataSetClient;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionOracle;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.common.base.Preconditions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Base test for OrderedColumnarTable.
 * @param <T> table type
 */
public abstract class OrderedColumnarTableTest<T extends OrderedColumnarTable> {
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

  protected TransactionSystemClient txClient;

  protected abstract T getTable(String name) throws Exception;
  protected abstract DataSetManager getTableManager() throws Exception;
  void closeTable(OrderedColumnarTable table) throws Exception {
    if (table != null && table instanceof DataSetClient) {
      ((DataSetClient) table).close();
    }
  }

  @Before
  public void before() {
    // todo: avoid this hack
    InMemoryTransactionOracle.reset();
    txClient = new InMemoryTxSystemClient();
  }

  @After
  public void after() {
    txClient = null;
  }

  @Test
  public void testBasicGetPutWithTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, $(C1), $(V1));
      // verify can see changes inside tx
      verify($(C1, V1), myTable1.get(R1, $(C1, C2)));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.start();
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify doesn't see changes of tx1
      verify($(), myTable2.get(R1, $(C1, C2)));
      // write r2->c2,v2 in tx2
      myTable2.put(R2, $(C2), $(V2));
      // verify can see own changes
      verify($(C2, V2), myTable2.get(R2, $(C1, C2)));

      // verify tx1 cannot see changes of tx2
      verify($(), myTable1.get(R2, $(C1, C2)));

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // start tx3 and verify that changes of tx1 are not visible yet (even though they are flushed)
      Transaction tx3 = txClient.start();
      OrderedColumnarTable myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      verify($(), myTable3.get(R1, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));
      // start tx4 and verify that changes of tx1 are now visible
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx4 = txClient.start();
      ((TransactionAware) myTable3).startTx(tx4);
      verify($(C1, V1), myTable3.get(R1, $(C1, C2)));

      // but tx2 still doesn't see committed changes of tx2
      verify($(), myTable2.get(R1, $(C1, C2)));
      // and tx4 doesn't see changes of tx2
      verify($(), myTable3.get(R2, $(C1, C2)));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // do change in tx2 that is conflicting with tx1
      myTable2.put(R1, $(C1), $(V2));
      // change is OK and visible inside tx2
      verify($(C1, V2), myTable2.get(R1, $(C1, C2)));

      // cannot commit: conflict should be detected
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));

      // rolling back tx2 changes and aborting tx
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // verifying that none of the changes of tx2 made it to be visible to other txs
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx5 = txClient.start();
      ((TransactionAware) myTable3).startTx(tx5);
      verify($(C1, V1), myTable3.get(R1, $(C1, C2)));
      verify($(), myTable3.get(R2, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      manager.drop("myTable");
    }
  }

  @Test
  public void testBasicCompareAndSwapWithTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, $(C1), $(V1));
      // write r1->c2,v2 but not commit
      Assert.assertTrue(myTable1.compareAndSwap(R1, C2, null, V5));
      // verify compare and swap result visible inside tx before commit
      verify($(C1, V1, C2, V5), myTable1.get(R1, $(C1, C2)));
      // these should fail
      Assert.assertFalse(myTable1.compareAndSwap(R1, C1, null, V1));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C1, V2, V1));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C2, null, V2));
      Assert.assertFalse(myTable1.compareAndSwap(R1, C2, V2, V1));
      // but this should succeed
      Assert.assertTrue(myTable1.compareAndSwap(R1, C2, V5, V2));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.start();

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // check that tx2 doesn't see changes (even though they were flushed) of tx1 by trying to compareAndSwap
      // assuming current value is null
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      Assert.assertTrue(myTable2.compareAndSwap(R1, C1, null, V3));

      // start tx3 and verify same thing again
      Transaction tx3 = txClient.start();
      OrderedColumnarTable myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      Assert.assertTrue(myTable3.compareAndSwap(R1, C1, null, V2));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));

      // start tx4 and verify that changes of tx1 are now visible
      Transaction tx4 = txClient.start();
      OrderedColumnarTable myTable4 = getTable("myTable");
      ((TransactionAware) myTable4).startTx(tx4);
      verify($(C1, V1, C2, V2), myTable4.get(R1, $(C1, C2)));

      // tx3 still cannot see tx1 changes
      Assert.assertTrue(myTable3.compareAndSwap(R1, C2, null, V5));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));

      // verify we can do some ops with tx4 based on data written with tx1
      Assert.assertFalse(myTable4.compareAndSwap(R1, C1, null, V4));
      Assert.assertFalse(myTable4.compareAndSwap(R1, C2, null, V5));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C1, V1, V3));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C2, V2, V4));
      myTable4.delete(R1, $(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // verifying the result contents in next transaction
      Transaction tx5 = txClient.start();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      verify($(C2, V4), myTable4.get(R1, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      manager.drop("myTable");
    }
  }

  @Test
  public void testBasicIncrementWithTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    OrderedColumnarTable myTable1 = null, myTable2 = null, myTable3 = null, myTable4 = null;
    try {
      Transaction tx1 = txClient.start();
      myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, $(C1), $(L4));
      verify($(C1), l(1L), myTable1.increment(R1, $(C1), l(-3L)));
      verify($(C2), l(2L), myTable1.increment(R1, $(C2), l(2L)));
      // verify increment result visible inside tx before commit
      verify($(C1, L1, C2, L2), myTable1.get(R1, $(C1, C2)));
      // incrementing non-long value should fail
      myTable1.put(R1, $(C5), $(V5));
      try {
        myTable1.increment(R1, $(C5), l(5L));
        Assert.assertTrue(false);
      } catch (OperationException e) {}
      // previous increment should not do any change
      verify($(C5, V5), myTable1.get(R1, $(C5)));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.start();

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // check that tx2 doesn't see changes (even though they were flushed) of tx1
      // assuming current value is null
      myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      verify($(), myTable2.get(R1, $(C1, C2, C5)));
      verify($(C1), l(55L), myTable2.increment(R1, $(C1), l(55L)));

      // start tx3 and verify same thing again
      Transaction tx3 = txClient.start();
      myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      verify($(), myTable3.get(R1, $(C1, C2, C5)));
      verify($(C1), l(4L), myTable3.increment(R1, $(C1), l(4L)));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));

      // start tx4 and verify that changes of tx1 are now visible
      Transaction tx4 = txClient.start();
      myTable4 = getTable("myTable");
      ((TransactionAware) myTable4).startTx(tx4);
      verify($(C1, L1, C2, L2, C5, V5), myTable4.get(R1, $(C1, C2, C3, C4, C5)));

      // tx3 still cannot see tx1 changes, only its own
      verify($(C1, L4), myTable3.get(R1, $(C1, C2, C5)));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));

      // verify we can do some ops with tx4 based on data written with tx1
      verify($(C1, C2, C3), l(3L, 3L, 5L), myTable4.increment(R1, $(C1, C2, C3), l(2L, 1L, 5L)));
      myTable4.delete(R1, $(C2));
      verify($(C4), l(3L), myTable4.increment(R1, $(C4), l(3L)));
      myTable4.delete(R1, $(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // verifying the result contents in next transaction
      Transaction tx5 = txClient.start();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      verify($(C3, L5, C4, L3, C5, V5), myTable4.get(R1, $(C1, C2, C3, C4, C5)));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      manager.drop("myTable");
    }
  }



  @Test
  public void testBasicDeleteWithTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      // write r1->c1,v1 and commit
      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, $(C1, C2), $(V1, V2));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test delete ops
      // start new tx2
      Transaction tx2 = txClient.start();
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify tx2 sees changes of tx1
      verify($(C1, V1, C2, V2), myTable2.get(R1, $(C1, C2)));
      // delete c1
      myTable2.delete(R1, $(C1));
      // verify can see deletes in own changes before commit
      verify($(C2, V2), myTable2.get(R1, $(C1, C2)));
      // overwrite c2 and write new value to c1
      myTable2.put(R1, $(C1, C2), $(V3, V4));
      // verify can see changes in own changes before commit
      verify($(C1, V3, C2, V4), myTable2.get(R1, $(C1, C2)));
      // delete c2
      myTable2.delete(R1, $(C2));
      // verify that delete is there (i.e. not reverted to whatever persisted)
      verify($(C1, V3), myTable2.get(R1, $(C1, C2)));

      // start tx3 and verify that changes of tx2 are not visible yet
      Transaction tx3 = txClient.start();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx3);
      verify($(C1, V1, C2, V2), myTable1.get(R1, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // starting tx4 before committing tx2 so that we can check conflicts are detected wrt deletes
      Transaction tx4 = txClient.start();
      // starting tx5 before committing tx2 so that we can check conflicts are detected wrt deletes
      Transaction tx5 = txClient.start();

      // commit tx2 in stages to see how races are handled wrt delete ops
      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());

      // start tx6 and verify that changes of tx2 are not visible yet (even though they are flushed)
      Transaction tx6 = txClient.start();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx6);
      verify($(C1, V1, C2, V2), myTable1.get(R1, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx6));

      // make tx2 visible
      Assert.assertTrue(txClient.commit(tx2));

      // start tx7 and verify that changes of tx2 are now visible
      Transaction tx7 = txClient.start();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx7);
      verify($(C1, V3), myTable1.get(R1, $(C1, C2)));
      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx7));

      // but not visible to tx4 that we started earlier than tx2 became visible
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx4);
      verify($(C1, V1, C2, V2), myTable1.get(R1, $(C1, C2)));

      // writing to deleted column, to check conflicts are detected (delete-write conflict)
      myTable1.put(R1, $(C2), $(V5));
      Assert.assertFalse(txClient.canCommit(tx4, ((TransactionAware) myTable1).getTxChanges()));

      // deleting changed column, to check conflicts are detected (write-delete conflict)
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx5);
      verify($(C1, V1, C2, V2), myTable1.get(R1, $(C1, C2)));
      myTable1.delete(R1, $(C1));
      Assert.assertFalse(txClient.canCommit(tx5, ((TransactionAware) myTable1).getTxChanges()));

    } finally {
      manager.drop("myTable");
    }
  }

  @Test
  public void testBasicScanWithTx() throws Exception {
    // todo: make work with tx well (merge with buffer, conflicts) and add tests for that
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      // write r1...r5 and commit
      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, $(C1), $(V1));
      myTable1.put(R2, $(C2), $(V2));
      myTable1.put(R3, $(C3, C4), $(V3, V4));
      myTable1.put(R4, $(C4), $(V4));
      myTable1.put(R5, $(C5), $(V5));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test scans
      // currently not testing races/conflicts/etc as this logic is not there for scans yet; so using one same tx
      Transaction tx2 = txClient.start();
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded scan
      verify($(R2, R3, R4),
             $$($(C2, V2),
                $(C3, V3, C4, V4),
                $(C4, V4)),
             myTable2.scan(R2, R5));
      // open start scan
      verify($(R1, R2, R3),
             $$($(C1, V1),
                $(C2, V2),
                $(C3, V3, C4, V4)),
             myTable2.scan(null, R4));
      // open end scan
      verify($(R3, R4, R5),
             $$($(C3, V3, C4, V4),
                $(C4, V4),
                $(C5, V5)),
             myTable2.scan(R3, null));
      // open ends scan
      verify($(R1, R2, R3, R4, R5),
             $$($(C1, V1),
                $(C2, V2),
                $(C3, V3, C4, V4),
                $(C4, V4),
                $(C5, V5)),
             myTable2.scan(null, null));

      // adding/changing/removing some columns
      myTable2.put(R2, $(C1, C2, C3), $(V4, V3, V2));
      myTable2.delete(R3, $(C4));

      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());
      Assert.assertTrue(txClient.commit(tx2));

      // Checking that changes are reflected in new scans in new tx
      Transaction tx3 = txClient.start();
      // NOTE: table can be re-used betweet tx
      ((TransactionAware) myTable1).startTx(tx3);

      verify($(R2, R3, R4),
             $$($(C1, V4, C2, V3, C3, V2),
                $(C3, V3),
                $(C4, V4)),
             myTable1.scan(R2, R5));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      manager.drop("myTable");
    }
  }

  @Test
  public void testBasicColumnRangeWithTx() throws Exception {
    // todo: test more tx logic (or add to get/put unit-test)
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {
      // write test data and commit
      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, $(C1, C2, C3, C4, C5), $(V1, V2, V3, V4, V5));
      myTable1.put(R2, $(C1), $(V2));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test column range get
      Transaction tx2 = txClient.start();
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded range
      verify($(C2, V2, C3, V3, C4, V4),
             myTable2.get(R1, C2, C5, Integer.MAX_VALUE));
      // open start range
      verify($(C1, V1, C2, V2, C3, V3),
             myTable2.get(R1, null, C4, Integer.MAX_VALUE));
      // open end range
      verify($(C3, V3, C4, V4, C5, V5),
             myTable2.get(R1, C3, null, Integer.MAX_VALUE));
      // open ends range
      verify($(C1, V1, C2, V2, C3, V3, C4, V4, C5, V5),
             myTable2.get(R1, null, null, Integer.MAX_VALUE));

      // same with limit
      // bounded range with limit
      verify($(C2, V2),
             myTable2.get(R1, C2, C5, 1));
      // open start range with limit
      verify($(C1, V1, C2, V2),
             myTable2.get(R1, null, C4, 2));
      // open end range with limit
      verify($(C3, V3, C4, V4),
             myTable2.get(R1, C3, null, 2));
      // open ends range with limit
      verify($(C1, V1, C2, V2, C3, V3, C4, V4),
             myTable2.get(R1, null, null, 4));

      // adding/changing/removing some columns
      myTable2.put(R1, $(C1, C2, C3), $(V4, V3, V2));
      myTable2.delete(R1, $(C4));

      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable2).commitTx());
      Assert.assertTrue(txClient.commit(tx2));

      // Checking that changes are reflected in new scans in new tx
      Transaction tx3 = txClient.start();
      // NOTE: table can be re-used betweet tx
      ((TransactionAware) myTable1).startTx(tx3);

      verify($(C2, V3, C3, V2),
             myTable1.get(R1, C2, C5, Integer.MAX_VALUE));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      manager.drop("myTable");
    }
  }

  @Test
  public void testMultiTableTx() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("table1");
    manager.create("table2");
    try {
      // test no conflict if changing different tables

      Transaction tx1 = txClient.start();
      OrderedColumnarTable table1 = getTable("table1");
      ((TransactionAware) table1).startTx(tx1);
      // write r1->c1,v1 but not commit
      table1.put(R1, $(C1), $(V1));

      // start new tx
      Transaction tx2 = txClient.start();
      OrderedColumnarTable table2 = getTable("table2");
      ((TransactionAware) table2).startTx(tx2);

      // change in tx2 same data but in different table
      table2.put(R1, $(C1), $(V2));

      // start new tx
      Transaction tx3 = txClient.start();
      OrderedColumnarTable table1_2 = getTable("table1");
      ((TransactionAware) table1_2).startTx(tx3);

      // change in tx3 same data in same table as tx1
      table1_2.put(R1, $(C1), $(V2));

      // committing tx1
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) table1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) table1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // no conflict should be when committing tx2
      Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) table2).getTxChanges()));

      // but conflict should be when committing tx3
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) table1_2).getTxChanges()));

    } finally {
      // NOTE: we are doing our best to cleanup junk between tests to isolate errors, but we are not going to be
      //       crazy about it
      manager.drop("table1");
      manager.drop("table2");
    }
  }

  @Test
  public void testRollingBackPersistedChanges() throws Exception {
    DataSetManager manager = getTableManager();
    manager.create("myTable");
    try {

      Transaction tx1 = txClient.start();
      OrderedColumnarTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, $(C1), $(V1));
      // verify can see changes inside tx
      verify($(C1, V1), myTable1.get(R1, $(C1)));

      // persisting changes
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // let's pretend that after persisting changes we still got conflict when finalizing tx, so
      // rolling back changes
      Assert.assertTrue(((TransactionAware) myTable1).rollbackTx());

      // making tx visible
      txClient.abort(tx1);

      // start new tx
      Transaction tx2 = txClient.start();
      OrderedColumnarTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify don't see rolled back changes
      verify($(), myTable2.get(R1, $(C1)));

    } finally {
      manager.drop("myTable");
    }
  }

  // todo: verify changing different cols of one row causes conflict
  // todo: check race: table committed, but txClient doesn't know yet (visibility, conflict detection)
  // todo: test overwrite + delete, that delete doesn't cause getting from persisted again

  void verify(byte[][] expected, OperationResult<Map<byte[], byte[]>> actual) {
    Preconditions.checkArgument(expected.length % 2 == 0, "expected [key,val] pairs in first param");
    if (expected.length == 0) {
      Assert.assertTrue(actual.isEmpty());
      return;
    }
    verify(expected, actual.getValue());
  }

  void verify(byte[][] expected, Map<byte[], byte[]> rowMap) {
    Assert.assertEquals(expected.length / 2, rowMap.size());
    for (int i = 0; i < expected.length; i += 2) {
      byte[] key = expected[i];
      byte[] val = expected[i + 1];
      Assert.assertArrayEquals(val, rowMap.get(key));
    }
  }

  void verify(byte[][] expectedColumns, long[] expectedValues, Map<byte[], Long> actual) {
    Assert.assertEquals(expectedColumns.length, actual.size());
    for (int i = 0; i < expectedColumns.length; i++) {
      Assert.assertEquals(expectedValues[i], (long) actual.get(expectedColumns[i]));
    }
  }

  void verify(byte[][] expectedRows, byte[][][] expectedRowMaps, Scanner scan) {
    for (int i = 0; i < expectedRows.length; i++) {
      ImmutablePair<byte[], Map<byte[], byte[]>> next = scan.next();
      Assert.assertArrayEquals(expectedRows[i], next.getFirst());
      verify(expectedRowMaps[i], next.getSecond());
    }

    // nothing is left in scan
    Assert.assertNull(scan.next());
  }

  static long[] l(long... elems) {
    return elems;
  }

  static byte[][] $(byte[]... elems) {
    return elems;
  }

  static byte[][][] $$(byte[][]... elems) {
    return elems;
  }
}
