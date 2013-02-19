package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class TestMemoryOracle {

  private static final Injector injector =
    Guice.createInjector(new DataFabricModules().getInMemoryModules());

  private static TransactionOracle newOracle() {
    return injector.getInstance(TransactionOracle.class);
  }
  static final byte[] a = { 'a' };
  static final byte[] b = { 'b' };
  static final byte[] c = { 'c' };
  static final byte[][] columns = { { 'x' } };

  // test that txids are increasing
  // test that new transaction includes itself for read
  // test that new transaction excludes in-progress transaction
  // test that after commit, read pointer includes transaction
  // test that after abort, excludes transaction
  // test that after remove, read pointer includes transaction
  @Test
  public void testIncreasingIds() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();

    // start a transaction, should be >0, visible to itself
    Transaction tx1 = oracle.startTransaction();
    long txid1 = tx1.getTransactionId();
    Assert.assertTrue(txid1 > 0);
    Assert.assertTrue(tx1.getReadPointer().isVisible(txid1));

    // start another transaction, should be > first transaction, visible to itself, exclude first one
    Transaction tx2 = oracle.startTransaction();
    long txid2 = tx2.getTransactionId();
    ReadPointer rp2 = tx2.getReadPointer();
    Assert.assertTrue(txid2 > txid1);
    Assert.assertTrue(rp2.isVisible(txid2));
    Assert.assertFalse(rp2.isVisible(txid1));

    // commit the first transaction, should remain invisible to tx2
    oracle.commitTransaction(txid1);
    Assert.assertFalse(tx2.getReadPointer().isVisible(txid1));

    // start another transaction, should be > second transaction, visible to itself, exclude second one
    Transaction tx3 = oracle.startTransaction();
    long txid3 = tx3.getTransactionId();
    ReadPointer rp3 = tx3.getReadPointer();
    Assert.assertTrue(txid3 > txid2);
    Assert.assertTrue(rp3.isVisible(txid1));
    Assert.assertFalse(rp3.isVisible(txid2));
    Assert.assertTrue(rp3.isVisible(txid3));

    // abort transaction 2, should remain invisible to new transactions
    oracle.abortTransaction(txid2);
    Transaction tx4 = oracle.startTransaction();
    long txid4 = tx4.getTransactionId();
    ReadPointer rp4 = tx4.getReadPointer();
    Assert.assertTrue(txid4 > txid3);
    Assert.assertTrue(rp4.isVisible(txid1));
    Assert.assertFalse(rp4.isVisible(txid2));
    Assert.assertFalse(rp4.isVisible(txid3));
    Assert.assertTrue(rp4.isVisible(txid4));

    // remove transaction 2, should become visible now
    oracle.removeTransaction(txid2);
    Transaction tx5 = oracle.startTransaction();
    long txid5 = tx5.getTransactionId();
    ReadPointer rp5 = tx5.getReadPointer();
    Assert.assertTrue(txid5 > txid3);
    Assert.assertTrue(rp5.isVisible(txid1));
    Assert.assertTrue(rp5.isVisible(txid2));
    Assert.assertFalse(rp5.isVisible(txid3));
    Assert.assertFalse(rp5.isVisible(txid4));
    Assert.assertTrue(rp5.isVisible(txid5));
  }

  // test the write conflict detection
  @Test
  public void testWriteConflict() throws OmidTransactionException {
    final String table = "t";

    TransactionOracle oracle = newOracle();

    // start four transactions with overlapping writes
    Transaction tx1 = oracle.startTransaction();
    long txid1 = tx1.getTransactionId();
    oracle.addToTransaction(txid1, Collections.singletonList((Undo)new UndoWrite(table, a, columns)));

    Transaction tx2 = oracle.startTransaction();
    long txid2 = tx2.getTransactionId();
    oracle.addToTransaction(txid2, Collections.singletonList((Undo)new UndoWrite(table, a, columns)));

    Transaction tx3 = oracle.startTransaction();
    long txid3 = tx3.getTransactionId();
    oracle.addToTransaction(txid3, Collections.singletonList((Undo)new UndoWrite(table, b, columns)));

    Transaction tx4 = oracle.startTransaction();
    long txid4 = tx4.getTransactionId();
    oracle.addToTransaction(txid4, Collections.singletonList((Undo)new UndoWrite(table, c, columns)));

    // now each transaction has started and performed one write
    // each one may now perform another wrote or commit right away

    // tx2 writes another one and commits -> success
    oracle.addToTransaction(txid2, Collections.singletonList((Undo)new UndoWrite(table, c, columns)));
    Assert.assertTrue(oracle.commitTransaction(txid2).isSuccess());

    // now tx1 writes another value and commits -> abort due to conflict on a
    oracle.addToTransaction(txid1, Collections.singletonList((Undo)new UndoWrite(table, b, columns)));
    TransactionResult txres1 = oracle.commitTransaction(txid1);
    Assert.assertFalse(txres1.isSuccess());
    List<Undo> undos1 = txres1.getUndos();
    Assert.assertEquals(2, undos1.size());
    assertUndo(undos1, new RowSet.Row(table, a));
    assertUndo(undos1, new RowSet.Row(table, b));

    // now tx3 commits. It has a conflict with tx1 on b, but tx1 failed -> success
    Assert.assertTrue(oracle.commitTransaction(txid3).isSuccess());

    // now tx4 commits. It has a conflict with tx2 and must fail
    TransactionResult txres4 = oracle.commitTransaction(txid4);
    Assert.assertFalse(txres4.isSuccess());
    List<Undo> undos4 = txres4.getUndos();
    Assert.assertEquals(1, undos4.size());
    assertUndo(undos4, new RowSet.Row(table, c));
  }

  private static void assertUndo(List<Undo> undos, RowSet.Row key) {
    for (Undo u : undos) {
      if (key.equals(u.getRow())) {
        return;
      }
    }
    Assert.fail("Key " + key.toString() + " is not in list of undos");
  }

  private static final List<Undo> noUndos = Lists.newArrayList();

  // test that invalid commit/abort/add/remove throws exception
  @Test(expected = OmidTransactionException.class)
  public void testAddToNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    oracle.addToTransaction(new Random().nextInt(1000), noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.commitTransaction(txid);
    oracle.addToTransaction(txid, noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.addToTransaction(txid, noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.removeTransaction(txid);
    oracle.addToTransaction(txid, noUndos);
  }

  @Test(expected = OmidTransactionException.class)
  public void testCommitNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    oracle.commitTransaction(new Random().nextInt(1000));
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.commitTransaction(txid);
    oracle.commitTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.commitTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.removeTransaction(txid);
    oracle.commitTransaction(txid);
  }

  @Test(expected = OmidTransactionException.class)
  public void testAbortNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    oracle.abortTransaction(new Random().nextInt(1000));
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.commitTransaction(txid);
    oracle.abortTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.abortTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.removeTransaction(txid);
    oracle.abortTransaction(txid);
  }

  @Test(expected = OmidTransactionException.class)
  public void testRemoveNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    oracle.removeTransaction(new Random().nextInt(1000));
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveInProgress() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.removeTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.commitTransaction(txid);
    oracle.removeTransaction(txid);
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    long txid = oracle.startTransaction().getTransactionId();
    oracle.abortTransaction(txid);
    oracle.removeTransaction(txid);
    oracle.removeTransaction(txid);
  }

  // test that conflict detection can distinguish table names
  @Test
  public void testConflictDetectionAcrossTables() throws OmidTransactionException {
    final String t1 = null;
    final String t2 = "tCDAT2";
    final String t3 = "tCDAT3";

    // start three transactions
    TransactionOracle oracle = newOracle();
    long tx1 = oracle.startTransaction().getTransactionId();
    long tx2 = oracle.startTransaction().getTransactionId();
    long tx3 = oracle.startTransaction().getTransactionId();

    // all transactions write the same row to different tables, one uses default table
    oracle.addToTransaction(tx1, Collections.singletonList((Undo)new UndoWrite(t1, a, new byte[][] { b } )));
    oracle.addToTransaction(tx2, Collections.singletonList((Undo)new UndoWrite(t2, a, new byte[][] { b } )));
    oracle.addToTransaction(tx3, Collections.singletonList((Undo)new UndoWrite(t3, a, new byte[][] { b } )));

    // commit all transactions, none should fail
    Assert.assertTrue(oracle.commitTransaction(tx1).isSuccess());
    Assert.assertTrue(oracle.commitTransaction(tx2).isSuccess());
    Assert.assertTrue(oracle.commitTransaction(tx3).isSuccess());
  }
}
