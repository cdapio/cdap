package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import com.continuuity.data2.transaction.persist.InMemoryTransactionStateStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InMemoryTransactionManagerTest extends TransactionSystemTest {

  static CConfiguration conf = CConfiguration.create();

  InMemoryTransactionManager txManager = null;

  @Override
  protected TransactionSystemClient getClient() {
    return new InMemoryTxSystemClient(txManager);
  }

  @Before
  public void before() {
    conf.setInt(InMemoryTransactionManager.CFG_TX_CLAIM_SIZE, 10);
    conf.setInt(Constants.Transaction.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    txManager = new InMemoryTransactionManager(conf, new InMemoryTransactionStateStorage());
    txManager.startAndWait();
  }

  @After
  public void after() {
    txManager.stopAndWait();
  }

  @Test
  public void testTransactionCleanup() throws InterruptedException {
    conf.setInt(Constants.Transaction.Manager.CFG_TX_CLEANUP_INTERVAL, 3); // no cleanup thread
    conf.setInt(Constants.Transaction.Manager.CFG_TX_TIMEOUT, 2);
    // using a new tx manager that cleans up
    InMemoryTransactionManager txm = new InMemoryTransactionManager(conf, new InMemoryTransactionStateStorage());
    txm.startAndWait();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // start a transaction and leave it open
      Transaction tx1 = txm.startShort();
      // start a long running transaction and leave it open
      Transaction tx2 = txm.startLong();
      Transaction tx3 = txm.startLong();
      // start and commit a bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        if (!(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })) && txm.commit(tx))) {
         txm.abort(tx);
        }
      }
      // all of these should still be in the committed set
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(10, txm.getCommittedSize());
      // sleep longer than the cleanup interval
      TimeUnit.SECONDS.sleep(5);
      // transaction should now be invalid
      Assert.assertEquals(1, txm.getInvalidSize());
      // run another transaction
      Transaction txx = txm.startShort();
      // verify the exclude
      Assert.assertFalse(txx.isVisible(tx1.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx2.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx3.getWritePointer()));
      // try to commit the last transaction that was started
      if (!(txm.canCommit(txx, Collections.singleton(new byte[] { 0x0a })) && txm.commit(txx))) {
        txm.abort(txx);
      }
      // now the committed change sets should be empty again
      Assert.assertEquals(0, txm.getCommittedSize());
      // try to commit this transaction
      Assert.assertFalse(txm.canCommit(tx1, Collections.singleton(new byte[] { 0x11 })));
      txm.abort(tx1);
      // abort should have removed from invalid
      Assert.assertEquals(0, txm.getInvalidSize());
      // run another bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        if (!(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })) && txm.commit(tx))) {
          txm.abort(tx);
        }
      }
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // commit tx2, abort tx3
      Assert.assertTrue(txm.commit(tx2));
      txm.abort(tx3);
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(1, txm.getExcludedListSize());
    } finally {
      txm.stopAndWait();
    }
  }
}
