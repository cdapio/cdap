/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Unit test for {@link AbstractTransactionContext}.
 * All tests are copied from tephra TransactionContextTest.
 */
public class TransactionContextTest {

  private static final byte[] A = { 'a' };
  private static final byte[] B = { 'b' };

  private static TransactionManager txManager;
  private static DummyTxClient txClient;

  private final DummyTxAware ds1 = new DummyTxAware();
  private final DummyTxAware ds2 = new DummyTxAware();

  @BeforeClass
  public static void setup() {
    txManager = new TransactionManager(new Configuration());
    txManager.startAndWait();
    txClient = new DummyTxClient(txManager);
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }

  private TransactionContext newTransactionContext(TransactionAware... txAwares) {
    return new SimpleTransactionContext(txClient, txAwares);
  }

  @Before
  public void resetTxAwares() {
    ds1.reset();
    ds2.reset();
  }

  @Test
  public void testSuccessful() throws TransactionFailureException, InterruptedException {
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction
    context.finish();
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testPostCommitFailure() throws TransactionFailureException, InterruptedException {
    ds1.failPostCommitTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail but without rollback as the failure happens post-commit
    try {
      context.finish();
      Assert.fail("post commit failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("post failure", e.getCause().getMessage());
    }
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testPersistFailure() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testPersistFalse() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = InduceFailure.ReturnFalse;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertNull(e.getCause()); // in this case, the ds simply returned false
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testPersistAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = InduceFailure.ThrowException;
    ds1.failRollbackTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testPersistAndRollbackFalse() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = InduceFailure.ReturnFalse;
    ds1.failRollbackTxOnce = InduceFailure.ReturnFalse;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertNull(e.getCause()); // in this case, the ds simply returned false
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testCommitFalse() throws TransactionFailureException, InterruptedException {
    txClient.failCommits = 1;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("commit failed - exception should be thrown");
    } catch (TransactionConflictException e) {
      Assert.assertNull(e.getCause());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testCanCommitFalse() throws TransactionFailureException, InterruptedException {
    txClient.failCanCommitOnce = true;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("commit failed - exception should be thrown");
    } catch (TransactionConflictException e) {
      Assert.assertNull(e.getCause());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testChangesAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failChangesTxOnce = InduceFailure.ThrowException;
    ds1.failRollbackTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("get changes failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("changes failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testStartAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failStartTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    try {
      context.start();
      Assert.fail("start failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("start failure", e.getCause().getMessage());
    }
    // verify both are not rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertFalse(ds2.started);
    Assert.assertFalse(ds1.checked);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testAddThenSuccess() throws TransactionFailureException, InterruptedException {
    TransactionContext context = newTransactionContext(ds1);
    // start transaction
    context.start();
    // add a change to ds1
    ds1.addChange(A);
    // add ds2 to the tx
    context.addTransactionAware(ds2);
    // add a change to ds2
    ds2.addChange(B);
    // commit transaction
    context.finish();
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testAddThenFailure() throws TransactionFailureException, InterruptedException {
    ds2.failCommitTxOnce = InduceFailure.ThrowException;

    TransactionContext context = newTransactionContext(ds1);
    // start transaction
    context.start();
    // add a change to ds1
    ds1.addChange(A);
    // add ds2 to the tx
    context.addTransactionAware(ds2);
    // add a change to ds2
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testAddThenRemoveSuccess() throws TransactionFailureException {
    TransactionContext context = newTransactionContext();

    context.start();
    Assert.assertTrue(context.addTransactionAware(ds1));
    ds1.addChange(A);

    try {
      context.removeTransactionAware(ds1);
      Assert.fail("Removal of TransactionAware should fails when there is active transaction.");
    } catch (IllegalStateException e) {
      // Expected
    }

    context.finish();

    Assert.assertTrue(context.removeTransactionAware(ds1));
    // Removing a TransactionAware not added before should returns false
    Assert.assertFalse(context.removeTransactionAware(ds2));

    // Verify ds1 is committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertFalse(ds1.rolledBack);

    // Verify nothing happen to ds2
    Assert.assertFalse(ds2.started);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertFalse(ds2.rolledBack);

    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testAndThenRemoveOnFailure() throws TransactionFailureException {
    ds1.failCommitTxOnce = InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext();

    context.start();
    Assert.assertTrue(context.addTransactionAware(ds1));
    ds1.addChange(A);

    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }

    Assert.assertTrue(context.removeTransactionAware(ds1));

    // Verify ds1 is rolled back
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertTrue(ds1.rolledBack);

    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testAbortFailureThrowsFailureException() throws TransactionFailureException {
    TransactionContext context = new SimpleTransactionContext(new FailingTxClient(txManager));

    context.start();
    Assert.assertTrue(context.addTransactionAware(ds1));
    ds1.addChange(A);
    try {
      context.finish();
      Assert.fail("Finish should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      // expected
    }
  }

  @Test
  public void testGetTxAwareNameFails() throws TransactionFailureException {

    // tests that under any scneario that can make a transaction fail, exceptions from
    // getTransactionAwareName() do not affect proper abort, and the transaction context's
    // state is clear (no current transaction) afterwards.
    TransactionContext context = newTransactionContext(ds1);

    ds1.failGetName = InduceFailure.ThrowException;
    // the txAware will throw exceptions whenever getTransactionAwareName() is called.
    // This is called in various failure scenarios. Test these scenarios one by one and check that
    // the tx context is still functional after that.

    // test failure during startTx()
    ds1.failStartTxOnce = InduceFailure.ThrowException;
    try {
      context.start();
      Assert.fail("Start should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("start failure", e.getCause().getMessage());
      Assert.assertNull(context.getCurrentTransaction());
    }

    // test failure during getTxChanges()
    ds1.failChangesTxOnce = InduceFailure.ThrowException;
    context.start();
    try {
      context.finish();
      Assert.fail("Get changes should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("changes failure", e.getCause().getMessage());
    }
    Assert.assertNull(context.getCurrentTransaction());

    // test failure during commitTx()
    ds1.failCommitTxOnce = InduceFailure.ThrowException;
    context.start();
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    Assert.assertNull(context.getCurrentTransaction());

    // test failure during rollbackTx()
    ds1.failRollbackTxOnce = InduceFailure.ThrowException;
    context.start();
    try {
      context.abort();
      Assert.fail("Rollback should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("rollback failure", e.getCause().getMessage());
    }
    Assert.assertNull(context.getCurrentTransaction());

    // test failure during postTxCommit()
    ds1.failPostCommitTxOnce = InduceFailure.ThrowException;
    context.start();
    try {
      context.finish();
      Assert.fail("Post Commit should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("post failure", e.getCause().getMessage());
    }
    Assert.assertNull(context.getCurrentTransaction());

    Assert.assertTrue(context.removeTransactionAware(ds1));
    context.start();
    context.finish();
    Assert.assertNull(context.getCurrentTransaction());
  }

  enum InduceFailure { NoFailure, ReturnFalse, ThrowException }

  static class DummyTxAware implements TransactionAware {

    Transaction tx;
    boolean started = false;
    boolean committed = false;
    boolean checked = false;
    boolean rolledBack = false;
    boolean postCommitted = false;
    List<byte[]> changes = Lists.newArrayList();

    InduceFailure failStartTxOnce = InduceFailure.NoFailure;
    InduceFailure failChangesTxOnce = InduceFailure.NoFailure;
    InduceFailure failCommitTxOnce = InduceFailure.NoFailure;
    InduceFailure failPostCommitTxOnce = InduceFailure.NoFailure;
    InduceFailure failRollbackTxOnce = InduceFailure.NoFailure;
    InduceFailure failGetName = InduceFailure.NoFailure;

    void addChange(byte[] key) {
      changes.add(key);
    }

    void reset() {
      tx = null;
      started = false;
      checked = false;
      committed = false;
      rolledBack = false;
      postCommitted = false;
      changes.clear();
    }

    @Override
    public void startTx(Transaction tx) {
      reset();
      started = true;
      this.tx = tx;
      if (failStartTxOnce == InduceFailure.ThrowException) {
        failStartTxOnce = InduceFailure.NoFailure;
        throw new RuntimeException("start failure");
      }
    }

    @Override
    public void updateTx(Transaction tx) {
      this.tx = tx;
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      checked = true;
      if (failChangesTxOnce == InduceFailure.ThrowException) {
        failChangesTxOnce = InduceFailure.NoFailure;
        throw new RuntimeException("changes failure");
      }
      return ImmutableList.copyOf(changes);
    }

    @Override
    public boolean commitTx() throws Exception {
      committed = true;
      if (failCommitTxOnce == InduceFailure.ThrowException) {
        failCommitTxOnce = InduceFailure.NoFailure;
        throw new RuntimeException("persist failure");
      }
      if (failCommitTxOnce == InduceFailure.ReturnFalse) {
        failCommitTxOnce = InduceFailure.NoFailure;
        return false;
      }
      return true;
    }

    @Override
    public void postTxCommit() {
      postCommitted = true;
      if (failPostCommitTxOnce == InduceFailure.ThrowException) {
        failPostCommitTxOnce = InduceFailure.NoFailure;
        throw new RuntimeException("post failure");
      }
    }

    @Override
    public boolean rollbackTx() throws Exception {
      rolledBack = true;
      if (failRollbackTxOnce == InduceFailure.ThrowException) {
        failRollbackTxOnce = InduceFailure.NoFailure;
        throw new RuntimeException("rollback failure");
      }
      if (failRollbackTxOnce == InduceFailure.ReturnFalse) {
        failRollbackTxOnce = InduceFailure.NoFailure;
        return false;
      }
      return true;
    }

    @Override
    public String getTransactionAwareName() {
      if (failGetName == InduceFailure.ThrowException) {
        throw new RuntimeException("persist failure");
      }
      return "dummy";
    }
  }

  private static final class DummyTxClient extends InMemoryTxSystemClient {

    private boolean failCanCommitOnce;
    private int failCommits;
    private CommitState state = CommitState.Started;

    private enum CommitState {
      Started, Committed, Aborted, Invalidated
    }

    @Inject
    DummyTxClient(TransactionManager txManager) {
      super(txManager);
    }

    @Override
    public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) throws TransactionNotInProgressException {
      throw new RuntimeException("This should never be called)");
    }

    @Override
    public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds) throws TransactionFailureException {
      if (failCanCommitOnce) {
        failCanCommitOnce = false;
        throw new TransactionConflictException(tx.getTransactionId(), null, null);
      } else {
        super.canCommitOrThrow(tx, changeIds);
      }
    }

    @Override
    public boolean commit(Transaction tx) throws TransactionNotInProgressException {
      throw new RuntimeException("This should never be called)");
    }

    @Override
    public void commitOrThrow(Transaction tx) throws TransactionFailureException {
      if (failCommits-- > 0) {
        throw new TransactionConflictException(tx.getTransactionId(), null, null);
      } else {
        state = CommitState.Committed;
        super.commitOrThrow(tx);
      }
    }

    @Override
    public Transaction startLong() {
      state = CommitState.Started;
      return super.startLong();
    }

    @Override
    public Transaction startShort() {
      state = CommitState.Started;
      return super.startShort();
    }

    @Override
    public Transaction startShort(int timeout) {
      state = CommitState.Started;
      return super.startShort(timeout);
    }

    @Override
    public void abort(Transaction tx) {
      state = CommitState.Aborted;
      super.abort(tx);
    }

    @Override
    public boolean invalidate(long tx) {
      state = CommitState.Invalidated;
      return super.invalidate(tx);
    }
  }

  // to simulate situations where the tx service is not available when finishing a transaction
  private static final class FailingTxClient extends InMemoryTxSystemClient {

    private FailingTxClient(TransactionManager txManager) {
      super(txManager);
    }

    @Override
    public boolean canCommit(Transaction transaction,
                             Collection<byte[]> collection) throws TransactionNotInProgressException {
      throw new RuntimeException();
    }

    @Override
    public void canCommitOrThrow(Transaction transaction,
                                 Collection<byte[]> collection) throws TransactionFailureException {
      throw new RuntimeException();
    }

    @Override
    public void abort(Transaction transaction) {
      throw new RuntimeException();
    }
  }

  /**
   * A transaction context that maintains {@link TransactionAware} in a simple list.
   */
  private static final class SimpleTransactionContext extends AbstractTransactionContext {

    private final Set<TransactionAware> txAwares;

    SimpleTransactionContext(TransactionSystemClient txClient, TransactionAware...txAwares) {
      super(txClient);
      this.txAwares = new LinkedHashSet<>(Arrays.asList(txAwares));
    }

    @Override
    protected Iterable<TransactionAware> getTransactionAwares() {
      return txAwares;
    }

    @Override
    protected boolean doAddTransactionAware(TransactionAware txAware) {
      return txAwares.add(txAware);
    }

    @Override
    protected boolean doRemoveTransactionAware(TransactionAware txAware) {
      return txAwares.remove(txAware);
    }
  }
}
