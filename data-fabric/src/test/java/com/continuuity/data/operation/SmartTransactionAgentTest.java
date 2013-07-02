package com.continuuity.data.operation;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.util.OperationUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SmartTransactionAgentTest {

  static OperationExecutor opex;

  /**
   * Sets up the in-memory operation executor and the data fabric.
   */
  @BeforeClass
  public static void setupDataFabric() {
    // use Guice to inject an in-memory opex
    final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());
    opex = injector.getInstance(OperationExecutor.class);
  }

  static SmartTransactionAgent newAgent() {
    return new SmartTransactionAgent(opex, OperationUtil.DEFAULT);
  }

  private static final byte[] a = { 'a' };
  private static final byte[] b = { 'b' };
  private static final byte[] x = { 'x' };
  private static final byte[] y = { 'y' };
  private static final byte[] one = { '1' };
  private static final byte[] two = { '2' };

  // test illegal states:
  // start, then start again
  @Test(expected = IllegalStateException.class)
  public void testStartStarted() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    agent.start();
  }

  // submit without start
  @Test(expected = IllegalStateException.class)
  public void testSubmitNotStarted() throws OperationException {
    newAgent().submit(new Write(a, x, one));
  }

  // execute without start
  @Test(expected = IllegalStateException.class)
  public void testExecuteNotStarted() throws OperationException {
    newAgent().execute(new Read(a, x));
  }

  // abort after abort - allowed
  @Test
  public void testAbortNotStarted() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    agent.abort();
    agent.abort();
  }

  // finish without start - allowed with warning
  @Test
  public void testFinishNotStarted() throws OperationException {
    newAgent().finish();
  }

  // submit after abort
  @Test(expected = IllegalStateException.class)
  public void testSubmitToAborted() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();

    newAgent().submit(new Write(a, x, one));
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    agent.abort();
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(1, agent.getFailedCount());

    newAgent().submit(new Write(a, x, one));
  }

  // submit after finish
  @Test(expected = IllegalStateException.class)
  public void testSubmitToFinished() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    newAgent().submit(new Write(a, x, one));
    agent.finish();
    newAgent().submit(new Write(a, x, one));
  }

  // execute after finish
  @Test(expected = IllegalStateException.class)
  public void testExecuteToFinished() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    newAgent().submit(new Write(a, x, one));
    agent.finish();
    newAgent().execute(new Read(a, x));
  }

  // submit after fail
  @Test(expected = IllegalStateException.class)
  public void testSubmitAfterFailure() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    newAgent().submit(new CompareAndSwap(a, x, x, one));
    agent.finish();
    newAgent().submit(new Write(a, x, one));
  }

  // execute after fail
  @Test(expected = IllegalStateException.class)
  public void testExecuteAfterFailure() throws OperationException {
    SmartTransactionAgent agent = newAgent();
    agent.start();
    newAgent().submit(new CompareAndSwap(a, x, x, one));
    agent.finish();
    newAgent().execute(new Read(a, x));
  }

  // start, read, finish
  @Test
  public void testStartReadFinish() throws OperationException {
    final String table = "tSRF";
    // write a value outside the smart xaction
    opex.commit(OperationUtil.DEFAULT, new Write(table, a, x, one));
    SmartTransactionAgent agent = newAgent();
    agent.start();
    // read back the value inside the smart xaction
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    agent.finish();
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    // verify correct value was read
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(one, result.getValue().get(x));
  }

  // start, read, write, finish
  @Test
  public void testStartReadWriteReadFinish() throws OperationException {
    final String table = "tSRWRF";
    // write a value outside the smart xaction
    opex.commit(OperationUtil.DEFAULT, new Write(table, a, x, one));
    SmartTransactionAgent agent = newAgent();
    agent.start();
    // read back the value inside the smart xaction and verify
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    Assert.assertArrayEquals(one, result.getValue().get(x));
    // write a new value
    agent.submit(new Write(table, a, x, two));
    // operation count should be the same
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    // read back in a new transaction, must see old value
    result = opex.execute(OperationUtil.DEFAULT, new Read(table, a, x));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    // read back within the smart transaction, must see new value
    result = agent.execute(new Read(table, a, x));
    Assert.assertEquals(2, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    Assert.assertArrayEquals(two, result.getValue().get(x));

    // finish/commit
    agent.finish();
    // succeeded count should go up by 1 (we did one write)
    Assert.assertEquals(3, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    // read back in a new transaction, must see new value
    result = opex.execute(OperationUtil.DEFAULT, new Read(table, a, x));
    Assert.assertArrayEquals(two, result.getValue().get(x));
  }

  // start, write, write batch, read, finish
  @Test
  public void testStartWriteBatchReadFinish() throws OperationException {
    final String table = "tRWBRF";
    SmartTransactionAgent agent = newAgent();
    agent.start();
    // write a new value
    agent.submit(new Write(table, a, x, one));
    // write a batch
    agent.submit(batch(new Write(table, a, y, two), new Write(table, b, x, two)));
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // read row a outside the transaction, should return nothing
    OperationResult<Map<byte[], byte[]>> result =
        opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());

    // read back row a inside the smart xaction and verify that it sees both x and y
    result = agent.execute(new ReadColumnRange(table, a, null, null));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    Assert.assertArrayEquals(one, result.getValue().get(x));
    Assert.assertArrayEquals(two, result.getValue().get(y));

    // read back in a new transaction, must still see nothing
    result = opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());

    // finish/commit
    agent.finish();
    // succeeded count up by 3 writes
    Assert.assertEquals(4, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    // read back in a new transaction, must see new values
    result = opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    Assert.assertArrayEquals(two, result.getValue().get(y));
    result = opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, b, null, null));
    Assert.assertArrayEquals(two, result.getValue().get(x));
  }

  // start, write, read, write, finish
  @Test
  public void testStartWriteReadWriteFinish() throws OperationException {
    final String table = "tWRWF";
    SmartTransactionAgent agent = newAgent();
    agent.start();

    // write a new value
    agent.submit(new Write(table, a, x, one));

    // read this value back in-xaction, must see value
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertArrayEquals(one, result.getValue().get(x));

    // write another value in a different column
    agent.submit(new Write(table, a, y, two));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // and finish the smart xaction
    agent.finish();
    Assert.assertEquals(3, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // read back in a new transaction, must see new values
    result = opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    Assert.assertArrayEquals(two, result.getValue().get(y));
  }

  // start, write, fail
  @Test
  public void testStartWriteFail() throws OperationException {
    final String table = "tSWF";
    SmartTransactionAgent agent = newAgent();
    agent.start();

    // write a new value
    agent.submit(new Write(table, a, x, one));
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // try to increment that, should fail
    try {
      agent.execute(new Increment(table, a, x, 1L));
      Assert.fail("Increment should have failed (not compatible)");
    } catch (OperationException e) {
      // expected
    }
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(2, agent.getFailedCount());

    // ensure the write was rolled back
    OperationResult<Map<byte[], byte[]>> result =
      opex.execute(OperationUtil.DEFAULT, new Read(table, a, x));
    Assert.assertTrue(result.isEmpty());

    // attempt to abort the agent, should be ok even though agent is already aborted
    agent.abort();
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(2, agent.getFailedCount());
  }

  // start, write, read, write, abort
  @Test
  public void testStartWriteReadWriteAbort() throws OperationException {
    final String table = "tSWF";
    SmartTransactionAgent agent = newAgent();
    agent.start();

    // write a new value
    agent.submit(new Write(table, a, x, one));
    Assert.assertEquals(0, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // read this value back in-xaction, must see value
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());
    Assert.assertArrayEquals(one, result.getValue().get(x));

    // write a new value to different column
    agent.submit(new Write(table, a, y, two));
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(0, agent.getFailedCount());

    // abort the agent
    agent.abort();
    Assert.assertEquals(1, agent.getSucceededCount());
    Assert.assertEquals(2, agent.getFailedCount());

    // ensure both writes were rolled back
    // read back in a new transaction, must still see nothing
    result = opex.execute(OperationUtil.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testRepeatedWriteReadThenCommit() throws OperationException {
    final String table = "tRWRTC";
    SmartTransactionAgent agent = newAgent();
    agent.start();

    // increment a column
    agent.submit(new Increment(table, a, x, 1L));
    // read back, expect 1
    Assert.assertArrayEquals(Bytes.toBytes(1L), agent.execute(new Read(table, a, x)).getValue().get(x));
    // increment again
    agent.submit(new Increment(table, a, x, 1L));
    // read back, expect 2
    Assert.assertArrayEquals(Bytes.toBytes(2L), agent.execute(new Read(table, a, x)).getValue().get(x));
    // commit
    agent.finish();
  }

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

  @Test
  public void testLimitsTriggerExecution() throws OperationException {
    final String table = "tLTE";
    final int sizeLimit = 10 * 1024;
    final int countLimit = 42;

    // set the size limit low and the count limit high, so we hit the size limit first
    SmartTransactionAgent agent = newAgent();
    agent.setSizeLimit(sizeLimit);
    agent.setCountLimit(100000);
    agent.start();

    // a write of 1K
    byte[] zero1000 = new byte[1000];
    WriteOperation write = new Write(table, a, x, zero1000);
    int size = write.getSize();
    int numRounds = sizeLimit / size;

    // verify that this gets executed after numRounds
    executeNtimes(agent, write, 0, numRounds);
    // to verify that the limit gets reset after execution, we repeat this
    executeNtimes(agent, write, numRounds + 1, numRounds);

    // now set the size limit high and the count limit low, so we hit the count limit first
    agent = newAgent();
    agent.setSizeLimit(100 * 1024 * 1024);
    agent.setCountLimit(countLimit);
    agent.start();

    // an increment of a counter
    write = new Increment(table, b, x, 1L);

    // verify that this gets executed after numRounds
    executeNtimes(agent, write, 0, countLimit);
    // to verify that the limit gets reset after execution, we repeat this
    executeNtimes(agent, write, countLimit + 1, countLimit);
  }

  // helper to submit a write n times, verify each time that it weas deferred,
  // then submit again and verify it got executed
  private void executeNtimes(SmartTransactionAgent agent, WriteOperation write, int before, int n)
    throws OperationException {
    // the first n time, before we hit the limit, the write must not happen
    for (int i = 0; i < n; i++) {
      agent.submit(write);
      Assert.assertEquals(before, agent.getExecutedCount());
    }
    // submitting the write again, it should now trigger execution
    agent.submit(write);
    Assert.assertEquals(before + n + 1, agent.getExecutedCount());
  }
}
