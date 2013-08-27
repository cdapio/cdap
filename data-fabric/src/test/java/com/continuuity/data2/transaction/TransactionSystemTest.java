package com.continuuity.data2.transaction;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public abstract class TransactionSystemTest {
  protected abstract TransactionSystemClient getClient();

  @Test
  public void testCommitRaceHandling() {
    TransactionSystemClient client1 = getClient();
    TransactionSystemClient client2 = getClient();

    Transaction tx1 = client1.start();
    Transaction tx2 = client2.start();

    List<byte[]> changeSet = Lists.newArrayList(Bytes.toBytes("one"), Bytes.toBytes("two"));
    Assert.assertTrue(client1.canCommit(tx1, changeSet));
    // second one also can commit even thought there are conflicts with first since first one hasn't committed yet
    Assert.assertTrue(client2.canCommit(tx2, changeSet));

    Assert.assertTrue(client1.commit(tx1));

    // now second one should not commit, since there are conflicts with tx1 that has been committed
    Assert.assertFalse(client2.commit(tx2));
  }
}
