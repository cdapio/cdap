package com.continuuity.data2.transaction.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.data2.transaction.TransactionSystemTest;
import com.google.common.io.Closeables;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

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
    conf.setInt(InMemoryTransactionManager.CFG_TX_CLAIM_SIZE, 10);
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
    txManager = new InMemoryTransactionManager(conf, new ZooKeeperPersistor(conf));
    txManager.init();
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
