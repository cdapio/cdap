package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import com.google.common.io.Closeables;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class InMemoryTransactionManagerTest extends TransactionSystemTest {

  static InMemoryZookeeper zk;
  static CConfiguration conf;

  InMemoryTransactionManager txManager = null;

  @BeforeClass
  public static void startZK() throws IOException, InterruptedException {
    zk = new InMemoryZookeeper();
    conf = CConfiguration.create();
    conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zk.getConnectionString());
  }

  @AfterClass
  public static void stopZK() throws IOException, InterruptedException {
    Closeables.closeQuietly(zk);
  }

  @Override
  protected TransactionSystemClient getClient() {
    return new InMemoryTxSystemClient(txManager);
  }

  @Before
  public void before() {
    conf.setInt(InMemoryTransactionManager.CFG_TX_CLAIM_SIZE, 10);
    conf.setInt(InMemoryTransactionManager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    txManager = new InMemoryTransactionManager(conf, new ZooKeeperPersistor(conf));
    txManager.init();
  }

  @After
  public void after() {
    txManager.close();
  }

  @Test
  public void testTransactionCleanup() throws InterruptedException {
    conf.setInt(InMemoryTransactionManager.CFG_TX_CLEANUP_INTERVAL, 3); // no cleanup thread
    conf.setInt(InMemoryTransactionManager.CFG_TX_TIMEOUT, 2);
    // using a new tx manager that cleans up
    InMemoryTransactionManager txm = new InMemoryTransactionManager(conf, new ZooKeeperPersistor(conf));
    txm.init();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // start a transaction and leave it open
      Transaction tx1 = txm.start();
      // start a long running transaction and leave it open
      Transaction tx2 = txm.start(null);
      Transaction tx3 = txm.start(null);
      // start and commit a bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.start();
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
      Transaction txx = txm.start();
      // verify the exclude
      Assert.assertEquals(tx1.getWritePointer(), txx.getExcludedList()[0]);
      Assert.assertEquals(tx2.getWritePointer(), txx.getExcludedList()[1]);
      Assert.assertEquals(tx3.getWritePointer(), txx.getExcludedList()[2]);
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
        Transaction tx = txm.start();
        if (!(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })) && txm.commit(tx))) {
          txm.abort(tx);
        }
      }
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // commit tx2, abort tx3
      Assert.assertTrue(txm.commit(tx2));
      Assert.assertTrue(txm.abort(tx3));
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(1, txm.getExcludedListSize());
    } finally {
      txm.close();
    }
  }

  @Test
  public void testZKPersistence() {
    final byte[] a = { 'a' };
    final byte[] b = { 'b' };
    // start a tx1, add a change A and commit
    Transaction tx1 = txManager.start();
    Assert.assertTrue(txManager.canCommit(tx1, Collections.singleton(a)));
    Assert.assertTrue(txManager.commit(tx1));
    // start a tx2 and add a change B
    Transaction tx2 = txManager.start();
    Assert.assertTrue(txManager.canCommit(tx2, Collections.singleton(b)));
    // start a tx3
    Transaction tx3 = txManager.start();
    // restart
    txManager.close();
    before(); // starts a new tx manager
    // commit tx2
    Assert.assertTrue(txManager.commit(tx2));
    // start another transaction, must be greater than tx3
    Transaction tx4 = txManager.start();
    Assert.assertTrue(tx4.getWritePointer() > tx3.getWritePointer());
    // tx1 must be visble from tx2, but tx3 and tx4 must not
    Assert.assertTrue(tx2.isVisible(tx1.getWritePointer()));
    Assert.assertFalse(tx2.isVisible(tx3.getWritePointer()));
    Assert.assertFalse(tx2.isVisible(tx4.getWritePointer()));
    // add same change for tx3
    Assert.assertFalse(txManager.canCommit(tx3, Collections.singleton(b)));
    // check visibility with new xaction
    Transaction tx5 = txManager.start();
    Assert.assertTrue(tx5.isVisible(tx1.getWritePointer()));
    Assert.assertTrue(tx5.isVisible(tx2.getWritePointer()));
    Assert.assertFalse(tx5.isVisible(tx3.getWritePointer()));
    Assert.assertFalse(tx5.isVisible(tx4.getWritePointer()));
    // can commit tx3?
    txManager.abort(tx3);
    txManager.abort(tx4);
    txManager.abort(tx5);
    // start new tx and verify its exclude list is empty
    Transaction tx6 = txManager.start();
    Assert.assertEquals(0, tx6.getExcludedList().length);
    txManager.abort(tx6);

    // now start 5 x claim size transactions
    Transaction tx = txManager.start();
    for (int i = 1; i < 50; i++) {
      tx = txManager.start();
    }
    // simulate crash by starting a new tx manager
    before();
    // get a new transaction and verify it is greater
    Transaction txAfter = txManager.start();
    Assert.assertTrue(txAfter.getWritePointer() > tx.getWritePointer());
  }
}
