package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.table.ConflictDetection;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Scanner;
import com.continuuity.data2.OperationResult;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Base test for OrderedTable.
 * @param <T> table type
 */
public abstract class OrderedTableTest<T extends OrderedTable> {
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

  protected abstract T getTable(String name, ConflictDetection conflictLevel) throws Exception;
  protected abstract DatasetAdmin getTableAdmin(String name) throws Exception;

  protected T getTable(String name) throws Exception {
    return getTable(name, ConflictDetection.ROW);
  }

  @Before
  public void before() {
    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);
  }

  @After
  public void after() {
    txClient = null;
  }

  @Test
  public void testCreate() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    Assert.assertFalse(admin.exists());
    admin.create();
    Assert.assertTrue(admin.exists());
    // creation of non-existing table should do nothing
    admin.create();
  }

  @Test
  public void testBasicGetPutWithTx() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // verify can see changes inside tx
      verify(a(C1, V1), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(null, myTable1.get(R1, C2));
      verify(null, myTable1.get(R2, C1));
      verify(a(C1, V1), myTable1.get(R1));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.startShort();
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify doesn't see changes of tx1
      verify(a(), myTable2.get(R1, a(C1, C2)));
      verify(null, myTable2.get(R1, C1));
      verify(null, myTable2.get(R1, C2));
      verify(a(), myTable2.get(R1));
      // write r2->c2,v2 in tx2
      myTable2.put(R2, a(C2), a(V2));
      // verify can see own changes
      verify(a(C2, V2), myTable2.get(R2, a(C1, C2)));
      verify(null, myTable2.get(R2, C1));
      verify(V2, myTable2.get(R2, C2));
      verify(a(C2, V2), myTable2.get(R2));

      // verify tx1 cannot see changes of tx2
      verify(a(), myTable1.get(R2, a(C1, C2)));
      verify(null, myTable1.get(R2, C1));
      verify(null, myTable1.get(R2, C2));
      verify(a(), myTable1.get(R2));

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // start tx3 and verify that changes of tx1 are not visible yet (even though they are flushed)
      Transaction tx3 = txClient.startShort();
      OrderedTable myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      verify(a(), myTable3.get(R1, a(C1, C2)));
      verify(null, myTable3.get(R1, C1));
      verify(null, myTable3.get(R1, C2));
      verify(a(), myTable3.get(R1));
      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));
      // start tx4 and verify that changes of tx1 are now visible
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx4 = txClient.startShort();
      ((TransactionAware) myTable3).startTx(tx4);
      verify(a(C1, V1), myTable3.get(R1, a(C1, C2)));
      verify(V1, myTable3.get(R1, C1));
      verify(null, myTable3.get(R1, C2));
      verify(a(C1, V1), myTable3.get(R1));

      // but tx2 still doesn't see committed changes of tx2
      verify(a(), myTable2.get(R1, a(C1, C2)));
      verify(null, myTable2.get(R1, C1));
      verify(null, myTable2.get(R1, C2));
      verify(a(), myTable2.get(R1));
      // and tx4 doesn't see changes of tx2
      verify(a(), myTable3.get(R2, a(C1, C2)));
      verify(null, myTable3.get(R2, C1));
      verify(null, myTable3.get(R2, C2));
      verify(a(), myTable3.get(R2));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // do change in tx2 that is conflicting with tx1
      myTable2.put(R1, a(C1), a(V2));
      // change is OK and visible inside tx2
      verify(a(C1, V2), myTable2.get(R1, a(C1, C2)));
      verify(V2, myTable2.get(R1, C1));
      verify(null, myTable2.get(R1, C2));
      verify(a(C1, V2), myTable2.get(R1));

      // cannot commit: conflict should be detected
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));

      // rolling back tx2 changes and aborting tx
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // verifying that none of the changes of tx2 made it to be visible to other txs
      // NOTE: table instance can be re-used in series of transactions
      Transaction tx5 = txClient.startShort();
      ((TransactionAware) myTable3).startTx(tx5);
      verify(a(C1, V1), myTable3.get(R1, a(C1, C2)));
      verify(V1, myTable3.get(R1, C1));
      verify(null, myTable3.get(R1, C2));
      verify(a(C1, V1), myTable3.get(R1));
      verify(a(), myTable3.get(R2, a(C1, C2)));
      verify(null, myTable3.get(R2, C1));
      verify(null, myTable3.get(R2, C2));
      verify(a(), myTable3.get(R2));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicCompareAndSwapWithTx() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // write r1->c2,v2 but not commit
      Assert.assertTrue(myTable1.compareAndSwap(R1, C2, null, V5));
      // verify compare and swap result visible inside tx before commit
      verify(a(C1, V1, C2, V5), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(V5, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, V1, C2, V5), myTable1.get(R1));
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
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      Assert.assertTrue(myTable2.compareAndSwap(R1, C1, null, V3));

      // start tx3 and verify same thing again
      Transaction tx3 = txClient.startShort();
      OrderedTable myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      Assert.assertTrue(myTable3.compareAndSwap(R1, C1, null, V2));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // start tx4 and verify that changes of tx1 are now visible
      Transaction tx4 = txClient.startShort();
      OrderedTable myTable4 = getTable("myTable");
      ((TransactionAware) myTable4).startTx(tx4);
      verify(a(C1, V1, C2, V2), myTable4.get(R1, a(C1, C2)));
      verify(V1, myTable4.get(R1, C1));
      verify(V2, myTable4.get(R1, C2));
      verify(null, myTable4.get(R1, C3));
      verify(a(C2, V2), myTable4.get(R1, a(C2)));
      verify(a(C1, V1, C2, V2), myTable4.get(R1));

      // tx3 still cannot see tx1 changes
      Assert.assertTrue(myTable3.compareAndSwap(R1, C2, null, V5));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      ((TransactionAware) myTable3).rollbackTx();
      txClient.abort(tx3);

      // verify we can do some ops with tx4 based on data written with tx1
      Assert.assertFalse(myTable4.compareAndSwap(R1, C1, null, V4));
      Assert.assertFalse(myTable4.compareAndSwap(R1, C2, null, V5));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C1, V1, V3));
      Assert.assertTrue(myTable4.compareAndSwap(R1, C2, V2, V4));
      myTable4.delete(R1, a(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // verifying the result contents in next transaction
      Transaction tx5 = txClient.startShort();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      verify(a(C2, V4), myTable4.get(R1, a(C1, C2)));
      verify(null, myTable4.get(R1, C1));
      verify(V4, myTable4.get(R1, C2));
      verify(a(C2, V4), myTable4.get(R1));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicIncrementWithTx() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    OrderedTable myTable1, myTable2, myTable3, myTable4;
    try {
      Transaction tx1 = txClient.startShort();
      myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1), a(L4));
      verify(a(C1), la(1L), myTable1.increment(R1, a(C1), la(-3L)));
      verify(a(C2), la(2L), myTable1.increment(R1, a(C2), la(2L)));
      // verify increment result visible inside tx before commit
      verify(a(C1, L1, C2, L2), myTable1.get(R1, a(C1, C2)));
      verify(L1, myTable1.get(R1, C1));
      verify(L2, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, L1), myTable1.get(R1, a(C1)));
      verify(a(C1, L1, C2, L2), myTable1.get(R1));
      // incrementing non-long value should fail
      myTable1.put(R1, a(C5), a(V5));
      try {
        myTable1.increment(R1, a(C5), la(5L));
        Assert.assertTrue(false);
      } catch (NumberFormatException e) {
        // Expected
      }
      // previous increment should not do any change
      verify(a(C5, V5), myTable1.get(R1, a(C5)));
      verify(V5, myTable1.get(R1, C5));

      // start new tx (doesn't see changes of the tx1)
      Transaction tx2 = txClient.startShort();

      // committing tx1 in stages to check races are handled well
      // * first, flush operations of table
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // check that tx2 doesn't see changes (even though they were flushed) of tx1
      // assuming current value is null
      myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      verify(a(), myTable2.get(R1, a(C1, C2, C5)));
      verify(null, myTable2.get(R1, C1));
      verify(null, myTable2.get(R1, C2));
      verify(null, myTable2.get(R1, C5));
      verify(a(), myTable2.get(R1));
      verify(a(C1), la(55L), myTable2.increment(R1, a(C1), la(55L)));

      // start tx3 and verify same thing again
      Transaction tx3 = txClient.startShort();
      myTable3 = getTable("myTable");
      ((TransactionAware) myTable3).startTx(tx3);
      verify(a(), myTable3.get(R1, a(C1, C2, C5)));
      verify(null, myTable3.get(R1, C1));
      verify(null, myTable3.get(R1, C2));
      verify(null, myTable3.get(R1, C5));
      verify(a(), myTable3.get(R1));
      verify(a(C1), la(4L), myTable3.increment(R1, a(C1), la(4L)));

      // * second, make tx visible
      Assert.assertTrue(txClient.commit(tx1));

      // verify that tx2 cannot commit because of the conflicts...
      Assert.assertFalse(txClient.canCommit(tx2, ((TransactionAware) myTable2).getTxChanges()));
      ((TransactionAware) myTable2).rollbackTx();
      txClient.abort(tx2);

      // start tx4 and verify that changes of tx1 are now visible
      Transaction tx4 = txClient.startShort();
      myTable4 = getTable("myTable");
      ((TransactionAware) myTable4).startTx(tx4);
      verify(a(C1, L1, C2, L2, C5, V5), myTable4.get(R1, a(C1, C2, C3, C4, C5)));
      verify(a(C2, L2), myTable4.get(R1, a(C2)));
      verify(L1, myTable4.get(R1, C1));
      verify(L2, myTable4.get(R1, C2));
      verify(null, myTable4.get(R1, C3));
      verify(V5, myTable4.get(R1, C5));
      verify(a(C1, L1, C5, V5), myTable4.get(R1, a(C1, C5)));
      verify(a(C1, L1, C2, L2, C5, V5), myTable4.get(R1));

      // tx3 still cannot see tx1 changes, only its own
      verify(a(C1, L4), myTable3.get(R1, a(C1, C2, C5)));
      verify(L4, myTable3.get(R1, C1));
      verify(null, myTable3.get(R1, C2));
      verify(null, myTable3.get(R1, C5));
      verify(a(C1, L4), myTable3.get(R1));
      // and it cannot commit because its changes cause conflicts
      Assert.assertFalse(txClient.canCommit(tx3, ((TransactionAware) myTable3).getTxChanges()));
      ((TransactionAware) myTable3).rollbackTx();
      txClient.abort(tx3);

      // verify we can do some ops with tx4 based on data written with tx1
      verify(a(C1, C2, C3), la(3L, 3L, 5L), myTable4.increment(R1, a(C1, C2, C3), la(2L, 1L, 5L)));
      myTable4.delete(R1, a(C2));
      verify(a(C4), la(3L), myTable4.increment(R1, a(C4), la(3L)));
      myTable4.delete(R1, a(C1));

      // committing tx4
      Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable4).commitTx());
      Assert.assertTrue(txClient.commit(tx4));

      // verifying the result contents in next transaction
      Transaction tx5 = txClient.startShort();
      // NOTE: table instance can be re-used in series of transactions
      ((TransactionAware) myTable4).startTx(tx5);
      verify(a(C3, L5, C4, L3, C5, V5), myTable4.get(R1, a(C1, C2, C3, C4, C5)));
      verify(null, myTable4.get(R1, C1));
      verify(null, myTable4.get(R1, C2));
      verify(L5, myTable4.get(R1, C3));
      verify(L3, myTable4.get(R1, C4));
      verify(V5, myTable4.get(R1, C5));
      verify(a(C3, L5, C4, L3, C5, V5), myTable4.get(R1));
      Assert.assertTrue(txClient.canCommit(tx5, ((TransactionAware) myTable3).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable3).commitTx());
      Assert.assertTrue(txClient.commit(tx5));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicDeleteWithTx() throws Exception {
    // we will test 3 different delete column ops and one delete row op
    // * delete column with delete
    // * delete column with put null value
    // * delete column with put byte[0] value
    // * delete row with delete
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      // write smth and commit
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
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
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify tx2 sees changes of tx1
      verify(a(C1, V1, C2, V2), myTable2.get(R1, a(C1, C2)));
      // verify tx2 sees changes of tx1
      verify(a(C1, V2, C2, V3), myTable2.get(R2));
      // delete c1, r2
      myTable2.delete(R1, a(C1));
      myTable2.delete(R2);
      // same as delete a column
      myTable2.put(R3, C1, null);
      // same as delete a column
      myTable2.put(R4, C1, new byte[0]);
      // verify can see deletes in own changes before commit
      verify(a(C2, V2), myTable2.get(R1, a(C1, C2)));
      verify(null, myTable2.get(R1, C1));
      verify(V2, myTable2.get(R1, C2));
      verify(a(), myTable2.get(R2));
      verify(a(C2, V4), myTable2.get(R3));
      verify(a(C2, V5), myTable2.get(R4));
      // overwrite c2 and write new value to c1
      myTable2.put(R1, a(C1, C2), a(V3, V4));
      myTable2.put(R2, a(C1, C2), a(V4, V5));
      myTable2.put(R3, a(C1, C2), a(V1, V2));
      myTable2.put(R4, a(C1, C2), a(V2, V3));
      // verify can see changes in own changes before commit
      verify(a(C1, V3, C2, V4), myTable2.get(R1, a(C1, C2, C3)));
      verify(V3, myTable2.get(R1, C1));
      verify(V4, myTable2.get(R1, C2));
      verify(null, myTable2.get(R1, C3));
      verify(a(C1, V4, C2, V5), myTable2.get(R2));
      verify(a(C1, V1, C2, V2), myTable2.get(R3));
      verify(a(C1, V2, C2, V3), myTable2.get(R4));
      // delete c2 and r2
      myTable2.delete(R1, a(C2));
      myTable2.delete(R2);
      myTable2.put(R1, C2, null);
      myTable2.put(R1, C2, new byte[0]);
      // verify that delete is there (i.e. not reverted to whatever was persisted before)
      verify(a(C1, V3), myTable2.get(R1, a(C1, C2)));
      verify(V3, myTable2.get(R1, C1));
      verify(null, myTable2.get(R1, C2));
      verify(a(), myTable2.get(R2));
      verify(V1, myTable2.get(R3, C1));
      verify(V2, myTable2.get(R4, C1));

      // start tx3 and verify that changes of tx2 are not visible yet
      Transaction tx3 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx3);
      verify(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(V2, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, V1, C2, V2), myTable1.get(R1));
      verify(a(C1, V2, C2, V3), myTable1.get(R2));
      verify(a(C1, V3, C2, V4), myTable1.get(R3));
      verify(a(C1, V4, C2, V5), myTable1.get(R4));
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

      // start tx6 and verify that changes of tx2 are not visible yet (even though they are flushed)
      Transaction tx6 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx6);
      verify(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(V2, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, V1, C2, V2), myTable1.get(R1));
      verify(a(C1, V2, C2, V3), myTable1.get(R2));
      verify(a(C1, V3, C2, V4), myTable1.get(R3));
      verify(a(C1, V4, C2, V5), myTable1.get(R4));

      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx6));

      // make tx2 visible
      Assert.assertTrue(txClient.commit(tx2));

      // start tx7 and verify that changes of tx2 are now visible
      Transaction tx7 = txClient.startShort();
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx7);
      verify(a(C1, V3), myTable1.get(R1, a(C1, C2)));
      verify(a(C1, V3), myTable1.get(R1));
      verify(V3, myTable1.get(R1, C1));
      verify(null, myTable1.get(R1, C2));
      verify(a(C1, V3), myTable1.get(R1, a(C1, C2)));
      verify(a(), myTable1.get(R2));
      verify(V1, myTable1.get(R3, C1));
      verify(V2, myTable1.get(R4, C1));

      Assert.assertTrue(txClient.canCommit(tx6, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx7));

      // but not visible to tx4 that we started earlier than tx2 became visible
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx4);
      verify(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(V2, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, V1, C2, V2), myTable1.get(R1));
      verify(a(C1, V2, C2, V3), myTable1.get(R2));
      verify(a(C1, V3, C2, V4), myTable1.get(R3));
      verify(a(C1, V4, C2, V5), myTable1.get(R4));

      // writing to deleted column, to check conflicts are detected (delete-write conflict)
      myTable1.put(R1, a(C2), a(V5));
      Assert.assertFalse(txClient.canCommit(tx4, ((TransactionAware) myTable1).getTxChanges()));
      ((TransactionAware) myTable1).rollbackTx();
      txClient.abort(tx4);

      // deleting changed column, to check conflicts are detected (write-delete conflict)
      // NOTE: table instance can be re-used between tx
      ((TransactionAware) myTable1).startTx(tx5);
      verify(a(C1, V1, C2, V2), myTable1.get(R1, a(C1, C2)));
      verify(V1, myTable1.get(R1, C1));
      verify(V2, myTable1.get(R1, C2));
      verify(null, myTable1.get(R1, C3));
      verify(a(C1, V1, C2, V2), myTable1.get(R1));
      verify(a(C1, V2, C2, V3), myTable1.get(R2));
      verify(a(C1, V3, C2, V4), myTable1.get(R3));
      verify(a(C1, V4, C2, V5), myTable1.get(R4));
      // NOTE: we are verifying conflict in one operation only. We may want to test each...
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
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      // write r1...r5 and commit
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
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
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded scan
      verify(a(R2, R3, R4),
             aa(a(C2, V2),
                a(C3, V3, C4, V4),
                a(C4, V4)),
             myTable2.scan(R2, R5));
      // open start scan
      verify(a(R1, R2, R3),
             aa(a(C1, V1),
                a(C2, V2),
                a(C3, V3, C4, V4)),
             myTable2.scan(null, R4));
      // open end scan
      verify(a(R3, R4, R5),
             aa(a(C3, V3, C4, V4),
                a(C4, V4),
                a(C5, V5)),
             myTable2.scan(R3, null));
      // open ends scan
      verify(a(R1, R2, R3, R4, R5),
             aa(a(C1, V1),
                a(C2, V2),
                a(C3, V3, C4, V4),
                a(C4, V4),
                a(C5, V5)),
             myTable2.scan(null, null));

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

      verify(a(R2, R3, R4),
             aa(a(C1, V4, C2, V3, C3, V2),
                a(C3, V3),
                a(C4, V4)),
             myTable1.scan(R2, R5));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testScanAndDelete() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      //
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
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

      verify(a(Bytes.toBytes("1_08a"), Bytes.toBytes("1_09b")),
             aa(a(C1, V1),
                a(C1, V1)),
             myTable1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

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

      verify(a(Bytes.toBytes("1_07a"), Bytes.toBytes("1_08b"), Bytes.toBytes("1_09c")),
             aa(a(C1, V1),
                a(C1, V1),
                a(C1, V1)),
             myTable1.scan(Bytes.toBytes("1_"), Bytes.toBytes("2_")));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testBasicColumnRangeWithTx() throws Exception {
    // todo: test more tx logic (or add to get/put unit-test)
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      // write test data and commit
      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      myTable1.put(R1, a(C1, C2, C3, C4, C5), a(V1, V2, V3, V4, V5));
      myTable1.put(R2, a(C1), a(V2));
      Assert.assertTrue(txClient.canCommit(tx1, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Now, we will test column range get
      Transaction tx2 = txClient.startShort();
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // bounded range
      verify(a(C2, V2, C3, V3, C4, V4),
             myTable2.get(R1, C2, C5, Integer.MAX_VALUE));
      // open start range
      verify(a(C1, V1, C2, V2, C3, V3),
             myTable2.get(R1, null, C4, Integer.MAX_VALUE));
      // open end range
      verify(a(C3, V3, C4, V4, C5, V5),
             myTable2.get(R1, C3, null, Integer.MAX_VALUE));
      // open ends range
      verify(a(C1, V1, C2, V2, C3, V3, C4, V4, C5, V5),
             myTable2.get(R1, null, null, Integer.MAX_VALUE));

      // same with limit
      // bounded range with limit
      verify(a(C2, V2),
             myTable2.get(R1, C2, C5, 1));
      // open start range with limit
      verify(a(C1, V1, C2, V2),
             myTable2.get(R1, null, C4, 2));
      // open end range with limit
      verify(a(C3, V3, C4, V4),
             myTable2.get(R1, C3, null, 2));
      // open ends range with limit
      verify(a(C1, V1, C2, V2, C3, V3, C4, V4),
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

      verify(a(C2, V3, C3, V2),
             myTable1.get(R1, C2, C5, Integer.MAX_VALUE));

      Assert.assertTrue(txClient.canCommit(tx3, ((TransactionAware) myTable1).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      admin.drop();
    }
  }

  @Test
  public void testTxUsingMultipleTables() throws Exception {
    getTableAdmin("table1").create();
    getTableAdmin("table2").create();
    getTableAdmin("table3").create();
    getTableAdmin("table4").create();

    try {
      // We will be changing:
      // * table1 and table2 in tx1
      // * table2 and table3 in tx2 << will conflict with first one
      // * table3 and table4 in tx3

      Transaction tx1 = txClient.startShort();
      Transaction tx2 = txClient.startShort();
      Transaction tx3 = txClient.startShort();

      // Write data in tx1 and commit
      OrderedTable table11 = getTable("table1");
      ((TransactionAware) table11).startTx(tx1);
      // write r1->c1,v1 but not commit
      table11.put(R1, a(C1), a(V1));
      OrderedTable table21 = getTable("table2");
      ((TransactionAware) table21).startTx(tx1);
      // write r1->c1,v2 but not commit
      table21.put(R1, a(C1), a(V2));
      // verify writes inside same tx
      verify(a(C1, V1), table11.get(R1, a(C1)));
      verify(a(C1, V2), table21.get(R1, a(C1)));
      // commit tx1
      Assert.assertTrue(txClient.canCommit(tx1, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table11).getTxChanges(),
                         ((TransactionAware) table21).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table11).commitTx());
      Assert.assertTrue(((TransactionAware) table21).commitTx());
      Assert.assertTrue(txClient.commit(tx1));

      // Write data in tx2 and check that cannot commit because of conflicts
      OrderedTable table22 = getTable("table2");
      ((TransactionAware) table22).startTx(tx2);
      // write r1->c1,v1 but not commit
      table22.put(R1, a(C1), a(V2));
      OrderedTable table32 = getTable("table3");
      ((TransactionAware) table32).startTx(tx2);
      // write r1->c1,v2 but not commit
      table32.put(R1, a(C1), a(V3));
      // verify writes inside same tx
      verify(a(C1, V2), table22.get(R1, a(C1)));
      verify(a(C1, V3), table32.get(R1, a(C1)));
      // try commit tx2
      Assert.assertFalse(txClient.canCommit(tx2, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table22).getTxChanges(),
                         ((TransactionAware) table32).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table22).rollbackTx());
      Assert.assertTrue(((TransactionAware) table32).rollbackTx());
      txClient.abort(tx2);

      // Write data in tx3 and check that can commit (no conflicts)
      OrderedTable table33 = getTable("table3");
      ((TransactionAware) table33).startTx(tx3);
      // write r1->c1,v1 but not commit
      table33.put(R1, a(C1), a(V3));
      OrderedTable table43 = getTable("table4");
      ((TransactionAware) table43).startTx(tx3);
      // write r1->c1,v2 but not commit
      table43.put(R1, a(C1), a(V4));
      // verify writes inside same tx
      verify(a(C1, V3), table33.get(R1, a(C1)));
      verify(a(C1, V4), table43.get(R1, a(C1)));
      // commit tx3
      Assert.assertTrue(txClient.canCommit(tx3, ImmutableList.copyOf(
        Iterables.concat(((TransactionAware) table33).getTxChanges(),
                         ((TransactionAware) table43).getTxChanges()))));
      Assert.assertTrue(((TransactionAware) table33).commitTx());
      Assert.assertTrue(((TransactionAware) table43).commitTx());
      Assert.assertTrue(txClient.commit(tx3));

    } finally {
      getTableAdmin("table1").drop();
      getTableAdmin("table2").drop();
      getTableAdmin("table3").drop();
      getTableAdmin("table4").drop();
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
    getTableAdmin("table1").create();
    getTableAdmin("table2").create();
    try {
      // 1) Test conflicts when using different tables

      Transaction tx1 = txClient.startShort();
      OrderedTable table11 = getTable("table1", level);
      ((TransactionAware) table11).startTx(tx1);
      // write r1->c1,v1 but not commit
      table11.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx2 = txClient.startShort();
      OrderedTable table22 = getTable("table2", level);
      ((TransactionAware) table22).startTx(tx2);

      // change in tx2 same data but in different table
      table22.put(R1, a(C1), a(V2));

      // start new tx
      Transaction tx3 = txClient.startShort();
      OrderedTable table13 = getTable("table1", level);
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
      OrderedTable table14 = getTable("table1", level);
      ((TransactionAware) table14).startTx(tx4);
      // write r1->c1,v1 but not commit
      table14.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx5 = txClient.startShort();
      OrderedTable table15 = getTable("table1", level);
      ((TransactionAware) table15).startTx(tx5);

      // change in tx5 same data but in different row
      table15.put(R2, a(C1), a(V2));

      // start new tx
      Transaction tx6 = txClient.startShort();
      OrderedTable table16 = getTable("table1", level);
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
      OrderedTable table17 = getTable("table1", level);
      ((TransactionAware) table17).startTx(tx7);
      // write r1->c1,v1 but not commit
      table17.put(R1, a(C1), a(V1));

      // start new tx
      Transaction tx8 = txClient.startShort();
      OrderedTable table18 = getTable("table1", level);
      ((TransactionAware) table18).startTx(tx8);

      // change in tx8 same data but in different column
      table18.put(R1, a(C2), a(V2));

      // start new tx
      Transaction tx9 = txClient.startShort();
      OrderedTable table19 = getTable("table1", level);
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
      getTableAdmin("table1").drop();
      getTableAdmin("table2").drop();
    }
  }

  @Test
  public void testRollingBackPersistedChanges() throws Exception {
    DatasetAdmin admin = getTableAdmin("myTable");
    admin.create();
    try {
      // write and commit one row/column
      Transaction tx0 = txClient.startShort();
      OrderedTable myTable0 = getTable("myTable");
      ((TransactionAware) myTable0).startTx(tx0);
      myTable0.put(R2, a(C2), a(V2));
      Assert.assertTrue(txClient.canCommit(tx0, ((TransactionAware) myTable0).getTxChanges()));
      Assert.assertTrue(((TransactionAware) myTable0).commitTx());
      Assert.assertTrue(txClient.commit(tx0));
      ((TransactionAware) myTable0).postTxCommit();

      Transaction tx1 = txClient.startShort();
      OrderedTable myTable1 = getTable("myTable");
      ((TransactionAware) myTable1).startTx(tx1);
      // write r1->c1,v1 but not commit
      myTable1.put(R1, a(C1), a(V1));
      // also overwrite the value from tx0
      myTable1.put(R2, a(C2), a(V3));
      // verify can see changes inside tx
      verify(a(C1, V1), myTable1.get(R1, a(C1)));

      // persisting changes
      Assert.assertTrue(((TransactionAware) myTable1).commitTx());

      // let's pretend that after persisting changes we still got conflict when finalizing tx, so
      // rolling back changes
      Assert.assertTrue(((TransactionAware) myTable1).rollbackTx());

      // making tx visible
      txClient.abort(tx1);

      // start new tx
      Transaction tx2 = txClient.startShort();
      OrderedTable myTable2 = getTable("myTable");
      ((TransactionAware) myTable2).startTx(tx2);

      // verify don't see rolled back changes
      verify(a(), myTable2.get(R1, a(C1)));
      // verify we still see the previous value
      verify(a(C2, V2), myTable2.get(R2, a(C2)));

    } finally {
      admin.drop();
    }
  }

  // this test ensures that an existing client survives the truncating or dropping and recreating of a table
  @Test
  public void testClientSurvivesTableReset() throws Exception {
    final String tableName = "survive";
    DatasetAdmin admin = getTableAdmin(tableName);
    admin.create();
    OrderedTable table = getTable(tableName);

    // write some values
    Transaction tx0 = txClient.startShort();
    ((TransactionAware) table).startTx(tx0);
    table.put(R1, a(C1), a(V1));
    Assert.assertTrue(txClient.canCommit(tx0, ((TransactionAware) table).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table).commitTx());
    Assert.assertTrue(txClient.commit(tx0));
    ((TransactionAware) table).postTxCommit();

    // verify
    Transaction tx1 = txClient.startShort();
    ((TransactionAware) table).startTx(tx1);
    verify(a(C1, V1), table.get(R1));

    // drop table and recreate
    admin.drop();
    admin.create();

    // verify can read but nothing there
    verify(a(), table.get(R1));
    txClient.abort(tx1); // only did read, safe to abort

    // create a new client and write another value
    OrderedTable table2 = getTable(tableName);
    Transaction tx2 = txClient.startShort();
    ((TransactionAware) table2).startTx(tx2);
    table2.put(R1, a(C2), a(V2));
    Assert.assertTrue(txClient.canCommit(tx2, ((TransactionAware) table2).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table2).commitTx());
    Assert.assertTrue(txClient.commit(tx2));
    ((TransactionAware) table2).postTxCommit();

    // verify it is visible
    Transaction tx3 = txClient.startShort();
    ((TransactionAware) table).startTx(tx3);
    verify(a(C2, V2), table.get(R1));

    // truncate table
    admin.truncate();

    // verify can read but nothing there
    verify(a(), table.get(R1));
    txClient.abort(tx3); // only did read, safe to abort

    // write again with other client
    Transaction tx4 = txClient.startShort();
    ((TransactionAware) table2).startTx(tx4);
    table2.put(R1, a(C3), a(V3));
    Assert.assertTrue(txClient.canCommit(tx4, ((TransactionAware) table2).getTxChanges()));
    Assert.assertTrue(((TransactionAware) table2).commitTx());
    Assert.assertTrue(txClient.commit(tx4));
    ((TransactionAware) table2).postTxCommit();

    // verify it is visible
    Transaction tx5 = txClient.startShort();
    ((TransactionAware) table).startTx(tx5);
    verify(a(C3, V3), table.get(R1));
    txClient.abort(tx5); // only did read, safe to abort

    // drop table
    admin.drop();
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
    Assert.assertFalse("result is empty, but expected " + expected.length / 2 + " columns", actual.isEmpty());
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

  void verify(byte[] expected, byte[] actual) {
    Assert.assertArrayEquals(expected, actual);
  }

  void verify(byte[][] expectedRows, byte[][][] expectedRowMaps, Scanner scan) {
    for (int i = 0; i < expectedRows.length; i++) {
      Row next = scan.next();
      Assert.assertArrayEquals(expectedRows[i], next.getRow());
      verify(expectedRowMaps[i], next.getColumns());
    }

    // nothing is left in scan
    Assert.assertNull(scan.next());
  }

  static long[] la(long... elems) {
    return elems;
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
