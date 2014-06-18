package com.continuuity.data2.transaction;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.continuuity.data2.transaction.persist.TransactionStateStorage;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

/**
 *
 */
public abstract class TransactionSystemTest {

  public static final byte[] C1 = Bytes.toBytes("change1");
  public static final byte[] C2 = Bytes.toBytes("change2");
  public static final byte[] C3 = Bytes.toBytes("change3");
  public static final byte[] C4 = Bytes.toBytes("change4");

  protected abstract TransactionSystemClient getClient() throws Exception;

  protected abstract TransactionStateStorage getStateStorage() throws Exception;

  @Test
  public void testCommitRaceHandling() throws Exception {
    TransactionSystemClient client1 = getClient();
    TransactionSystemClient client2 = getClient();

    Transaction tx1 = client1.startShort();
    Transaction tx2 = client2.startShort();

    Assert.assertTrue(client1.canCommit(tx1, asList(C1, C2)));
    // second one also can commit even thought there are conflicts with first since first one hasn't committed yet
    Assert.assertTrue(client2.canCommit(tx2, asList(C2, C3)));

    Assert.assertTrue(client1.commit(tx1));

    // now second one should not commit, since there are conflicts with tx1 that has been committed
    Assert.assertFalse(client2.commit(tx2));
  }

  @Test
  public void testMultipleCommitsAtSameTime() throws Exception {
    // We want to check that if two txs finish at same time (wrt tx manager) they do not overwrite changesets of each
    // other in tx manager used for conflicts detection (we had this bug)
    // NOTE: you don't have to use multiple clients for that
    TransactionSystemClient client1 = getClient();
    TransactionSystemClient client2 = getClient();
    TransactionSystemClient client3 = getClient();
    TransactionSystemClient client4 = getClient();
    TransactionSystemClient client5 = getClient();

    Transaction tx1 = client1.startShort();
    Transaction tx2 = client2.startShort();
    Transaction tx3 = client3.startShort();
    Transaction tx4 = client4.startShort();
    Transaction tx5 = client5.startShort();

    Assert.assertTrue(client1.canCommit(tx1, asList(C1)));
    Assert.assertTrue(client1.commit(tx1));

    Assert.assertTrue(client2.canCommit(tx2, asList(C2)));
    Assert.assertTrue(client2.commit(tx2));

    // verifying conflicts detection
    Assert.assertFalse(client3.canCommit(tx3, asList(C1)));
    Assert.assertFalse(client4.canCommit(tx4, asList(C2)));
    Assert.assertTrue(client5.canCommit(tx5, asList(C3)));
  }

  @Test
  public void testCommitTwice() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommit(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // cannot commit twice same tx
    try {
      Assert.assertFalse(client.commit(tx));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
  }

  @Test
  public void testAbortTwice() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommit(tx, asList(C1, C2)));
    client.abort(tx);
    // abort of not active tx has no affect
    client.abort(tx);
  }

  @Test
  public void testReuseTx() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommit(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // can't re-use same tx again
    try {
      client.canCommit(tx, asList(C3, C4));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(tx));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }

    // abort of not active tx has no affect
    client.abort(tx);
  }

  @Test
  public void testUseNotStarted() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx1 = client.startShort();
    Assert.assertTrue(client.commit(tx1));

    // we know this is one is older than current writePointer and was not used
    Transaction txOld = new Transaction(tx1.getReadPointer(), tx1.getWritePointer() - 1,
                                        new long[] {}, new long[] {}, Transaction.NO_TX_IN_PROGRESS);
    try {
      Assert.assertFalse(client.canCommit(txOld, asList(C3, C4)));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(txOld));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    // abort of not active tx has no affect
    client.abort(txOld);

    // we know this is one is newer than current readPointer and was not used
    Transaction txNew = new Transaction(tx1.getReadPointer(), tx1.getWritePointer() + 1,
                                        new long[] {}, new long[] {}, Transaction.NO_TX_IN_PROGRESS);
    try {
      Assert.assertFalse(client.canCommit(txNew, asList(C3, C4)));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(txNew));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    // abort of not active tx has no affect
    client.abort(txNew);
  }

  @Test
  public void testAbortAfterCommit() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommit(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // abort of not active tx has no affect
    client.abort(tx);
  }

  // todo add test invalidate method
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient client = getClient();
    // Invalidate an in-progress tx
    Transaction tx1 = client.startShort();
    client.canCommit(tx1, asList(C1, C2));
    Assert.assertTrue(client.invalidate(tx1.getWritePointer()));
    // Cannot invalidate a committed tx
    Transaction tx2 = client.startShort();
    client.canCommit(tx2, asList(C3, C4));
    client.commit(tx2);
    Assert.assertFalse(client.invalidate(tx2.getWritePointer()));
  }

  @Test
  public void testResetState() throws Exception {
    // have tx in progress, committing and committed then reset,
    // get the last snapshot and see that it is empty
    TransactionSystemClient client = getClient();
    TransactionStateStorage stateStorage = getStateStorage();

    Transaction tx1 = client.startShort();
    Transaction tx2 = client.startShort();
    client.canCommit(tx1, asList(C1, C2));
    client.commit(tx1);
    client.canCommit(tx2, asList(C3, C4));

    long currentTs = System.currentTimeMillis();
    client.resetState();

    TransactionSnapshot snapshot = stateStorage.getLatestSnapshot();
    Assert.assertTrue(snapshot.getTimestamp() >= currentTs);
    Assert.assertEquals(0, snapshot.getInvalid().size());
    Assert.assertEquals(0, snapshot.getInProgress().size());
    Assert.assertEquals(0, snapshot.getCommittingChangeSets().size());
    Assert.assertEquals(0, snapshot.getCommittedChangeSets().size());
  }

  private Collection<byte[]> asList(byte[]... val) {
    return Arrays.asList(val);
  }
}
