package com.continuuity.data.operation.executor.omid;

import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
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
  private static final byte[] a = { 'a' };
  private static final byte[] b = { 'b' };
  private static final byte[] c = { 'c' };
  private static final byte[][] columns = { { 'x' } };

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
    Transaction tx1 = oracle.startTransaction(true);
    long txid1 = tx1.getWriteVersion();
    Assert.assertTrue(txid1 > 0);
    Assert.assertTrue(tx1.getReadPointer().isVisible(txid1));

    // start another transaction, should be > first transaction, visible to itself, exclude first one
    Transaction tx2 = oracle.startTransaction(true);
    long txid2 = tx2.getWriteVersion();
    ReadPointer rp2 = tx2.getReadPointer();
    Assert.assertTrue(txid2 > txid1);
    Assert.assertTrue(rp2.isVisible(txid2));
    Assert.assertFalse(rp2.isVisible(txid1));

    // commit the first transaction, should remain invisible to tx2
    oracle.commitTransaction(tx1);
    Assert.assertFalse(tx2.getReadPointer().isVisible(txid1));

    // start another transaction, should be > second transaction, visible to itself, exclude second one
    Transaction tx3 = oracle.startTransaction(true);
    long txid3 = tx3.getWriteVersion();
    ReadPointer rp3 = tx3.getReadPointer();
    Assert.assertTrue(txid3 > txid2);
    Assert.assertTrue(rp3.isVisible(txid1));
    Assert.assertFalse(rp3.isVisible(txid2));
    Assert.assertTrue(rp3.isVisible(txid3));

    // abort transaction 2, should remain invisible to new transactions
    oracle.abortTransaction(tx2);
    Transaction tx4 = oracle.startTransaction(true);
    long txid4 = tx4.getWriteVersion();
    ReadPointer rp4 = tx4.getReadPointer();
    Assert.assertTrue(txid4 > txid3);
    Assert.assertTrue(rp4.isVisible(txid1));
    Assert.assertFalse(rp4.isVisible(txid2));
    Assert.assertFalse(rp4.isVisible(txid3));
    Assert.assertTrue(rp4.isVisible(txid4));

    // remove transaction 2, should become visible now
    oracle.removeTransaction(tx2);
    Transaction tx5 = oracle.startTransaction(true);
    long txid5 = tx5.getWriteVersion();
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
    Transaction tx1 = oracle.startTransaction(true);
    oracle.addToTransaction(tx1, Collections.singletonList((Undo) new UndoWrite(table, a, columns)));

    Transaction tx2 = oracle.startTransaction(true);
    oracle.addToTransaction(tx2, Collections.singletonList((Undo) new UndoWrite(table, a, columns)));

    Transaction tx3 = oracle.startTransaction(true);
    oracle.addToTransaction(tx3, Collections.singletonList((Undo) new UndoWrite(table, b, columns)));

    Transaction tx4 = oracle.startTransaction(true);
    oracle.addToTransaction(tx4, Collections.singletonList((Undo) new UndoWrite(table, c, columns)));

    // now each transaction has started and performed one write
    // each one may now perform another wrote or commit right away

    // tx2 writes another one and commits -> success
    oracle.addToTransaction(tx2, Collections.singletonList((Undo) new UndoWrite(table, c, columns)));
    Assert.assertTrue(oracle.commitTransaction(tx2).isSuccess());

    // now tx1 writes another value and commits -> abort due to conflict on a
    oracle.addToTransaction(tx1, Collections.singletonList((Undo) new UndoWrite(table, b, columns)));
    TransactionResult txres1 = oracle.commitTransaction(tx1);
    Assert.assertFalse(txres1.isSuccess());
    List<Undo> undos1 = txres1.getUndos();
    Assert.assertEquals(2, undos1.size());
    assertUndo(undos1, new RowSet.Row(table, a));
    assertUndo(undos1, new RowSet.Row(table, b));

    // now tx3 commits. It has a conflict with tx1 on b, but tx1 failed -> success
    Assert.assertTrue(oracle.commitTransaction(tx3).isSuccess());

    // now tx4 commits. It has a conflict with tx2 and must fail
    TransactionResult txres4 = oracle.commitTransaction(tx4);
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
    int txid = new Random().nextInt(1000);
    oracle.addToTransaction(new Transaction(txid, new MemoryReadPointer(txid), true), noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.commitTransaction(tx);
    oracle.addToTransaction(tx, noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.addToTransaction(tx, noUndos);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAddToRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.removeTransaction(tx);
    oracle.addToTransaction(tx, noUndos);
  }

  @Test(expected = OmidTransactionException.class)
  public void testCommitNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    int txid = new Random().nextInt(1000);
    oracle.commitTransaction(new Transaction(txid, new MemoryReadPointer(txid), true));
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.commitTransaction(tx);
    oracle.commitTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.commitTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testCommitRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.removeTransaction(tx);
    oracle.commitTransaction(tx);
  }

  @Test(expected = OmidTransactionException.class)
  public void testAbortNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    int txid = new Random().nextInt(1000);
    oracle.abortTransaction(new Transaction(txid, new MemoryReadPointer(txid), true));
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.commitTransaction(tx);
    oracle.abortTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortAborted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.abortTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testAbortRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.removeTransaction(tx);
    oracle.abortTransaction(tx);
  }

  @Test(expected = OmidTransactionException.class)
  public void testRemoveNonexistent() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    int txid = new Random().nextInt(1000);
    oracle.removeTransaction(new Transaction(txid, new MemoryReadPointer(txid), true));
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveInProgress() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.removeTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveCommitted() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.commitTransaction(tx);
    oracle.removeTransaction(tx);
  }
  @Test(expected = OmidTransactionException.class)
  public void testRemoveRemoved() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(true);
    oracle.abortTransaction(tx);
    oracle.removeTransaction(tx);
    oracle.removeTransaction(tx);
  }

  // test that conflict detection can distinguish table names
  @Test
  public void testConflictDetectionAcrossTables() throws OmidTransactionException {
    final String t1 = null;
    final String t2 = "tCDAT2";
    final String t3 = "tCDAT3";

    // start three transactions
    TransactionOracle oracle = newOracle();
    Transaction tx1 = oracle.startTransaction(true);
    Transaction tx2 = oracle.startTransaction(true);
    Transaction tx3 = oracle.startTransaction(true);

    // all transactions write the same row to different tables, one uses default table
    oracle.addToTransaction(tx1, Collections.singletonList((Undo) new UndoWrite(t1, a, new byte[][] { b })));
    oracle.addToTransaction(tx2, Collections.singletonList((Undo) new UndoWrite(t2, a, new byte[][] { b })));
    oracle.addToTransaction(tx3, Collections.singletonList((Undo) new UndoWrite(t3, a, new byte[][] { b })));

    // commit all transactions, none should fail
    Assert.assertTrue(oracle.commitTransaction(tx1).isSuccess());
    Assert.assertTrue(oracle.commitTransaction(tx2).isSuccess());
    Assert.assertTrue(oracle.commitTransaction(tx3).isSuccess());
  }

  @Test
  public void testInitReadPointer() throws OmidTransactionException {
    TransactionOracle oracle1 = newOracle();
    Transaction transaction = oracle1.startTransaction(true);
    oracle1.commitTransaction(transaction);
    ReadPointer readPointer1 = oracle1.getReadPointer();

    // checking that whenever we start app again after the previous run we get read pointer which is
    // logically *not before* the last known read pointer from the previous oracle. I.e. so that with
    // new oracle we can see all writes happened with previous oracle.
    TransactionOracle oracle2 = newOracle();
    ReadPointer readPointer2 = oracle2.getReadPointer();
    Assert.assertTrue(readPointer1.getMaximum() <= readPointer2.getMaximum());
    readPointer2.isVisible(transaction.getWriteVersion());
  }

  // Testing not-tracking changes/conflicts transactions (NTC tx). In this case we are interested in:
  // 1) no conflicts raised with NTC transaction:
  //   1a) NTC transaction commits even if there's a write conflict with another one
  //   1b) normal tx commits even if there's a write conflict with NTC, writes are overridden
  //   1c) NTC transaction commits even if there's a write conflict with another NTC one
  // 2) NTC transaction didn't rollback changes on abort and hence it should stay in excluded list

  // 1)
  @Test
  public void testWriteConflictNotDetectedOnNonTrackingChangesTxCommit() throws OmidTransactionException {
    final String table = "t";

    TransactionOracle oracle = newOracle();

    // start four transactions with overlapping writes
    Transaction tx1 = oracle.startTransaction(false);
    oracle.addToTransaction(tx1, Collections.singletonList((Undo) new UndoWrite(table, a, columns)));

    Transaction tx2 = oracle.startTransaction(true);
    oracle.addToTransaction(tx2, Collections.singletonList((Undo) new UndoWrite(table, a, columns)));

    Transaction tx3 = oracle.startTransaction(true);
    oracle.addToTransaction(tx3, Collections.singletonList((Undo) new UndoWrite(table, b, columns)));

    Transaction tx4 = oracle.startTransaction(false);
    oracle.addToTransaction(tx4, Collections.singletonList((Undo) new UndoWrite(table, b, columns)));

    // now each transaction has started and performed one write
    // each one may now perform another wrote or commit right away

    // tx2 writes another one and commits -> success
    oracle.addToTransaction(tx2, Collections.singletonList((Undo) new UndoWrite(table, c, columns)));
    Assert.assertTrue(oracle.commitTransaction(tx2).isSuccess());

    // now NTC tx1 writes another value and commits -> success even though there's a conflict with tx2 (on 'a' row)
    oracle.addToTransaction(tx1, Collections.singletonList((Undo) new UndoWrite(table, b, columns)));
    Assert.assertTrue(oracle.commitTransaction(tx1).isSuccess());

    // now tx3 commits. It has a conflict with tx1 (on 'b' row) but will not fail since tx1 is NTC
    Assert.assertTrue(oracle.commitTransaction(tx3).isSuccess());

    // now tx4 commits. It has a conflict with tx1 (on 'b' row) but will not fail since tx4 and tx1 are NTC
    Assert.assertTrue(oracle.commitTransaction(tx4).isSuccess());
  }

  // 2)
  @Test
  public void testNonTrackingChangesTxStaysInExcludedOnAbort() throws OmidTransactionException {
    TransactionOracle oracle = newOracle();
    Transaction tx = oracle.startTransaction(false);
    oracle.abortTransaction(tx);
    oracle.removeTransaction(tx);
    Assert.assertFalse(oracle.getReadPointer().isVisible(tx.getWriteVersion()));
  }

}
