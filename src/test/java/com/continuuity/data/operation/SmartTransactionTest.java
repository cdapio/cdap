package com.continuuity.data.operation;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.collect.Lists;
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
public class SmartTransactionTest {

  static OperationExecutor opex;

  /**
   * Sets up the in-memory operation executor and the data fabric
   */
  @BeforeClass
  public static void setupDataFabric() {
    // use Guice to inject an in-memory opex
    final Injector injector =
      Guice.createInjector(new DataFabricModules().getInMemoryModules());
    opex = injector.getInstance(OperationExecutor.class);
  }

  static SmartTransactionAgent newAgent() {
    return new SmartTransactionAgent(opex, OperationContext.DEFAULT);
  }

  static final byte[] a = { 'a' };
  static final byte[] b = { 'b' };
  static final byte[] x = { 'x' };
  static final byte[] y = { 'y' };
  static final byte[] one = { '1' };
  static final byte[] two = { '2' };
  static final byte[] six = { '6' };


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
    agent.abort();
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
    opex.execute(OperationContext.DEFAULT, new Write(table, a, x, one));
    SmartTransactionAgent agent = newAgent();
    agent.start();
    // read back the value inside the smart xaction
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    agent.finish();
    // verify correct value was read
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(one, result.getValue().get(x));
  }

  // start, read, write, finish
  @Test
  public void testStartReadWriteReadFinish() throws OperationException {
    final String table = "tSRWRF";
    // write a value outside the smart xaction
    opex.execute(OperationContext.DEFAULT, new Write(table, a, x, one));
    SmartTransactionAgent agent = newAgent();
    agent.start();
    // read back the value inside the smart xaction and verify
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    // write a new value
    agent.submit(new Write(table, a, x, two));
    // read back in a new transaction, must see old value
    result = opex.execute(OperationContext.DEFAULT, new Read(table, a, x));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    // read back within the smart transaction, must see new value
    result = agent.execute(new Read(table, a, x));
    Assert.assertArrayEquals(two, result.getValue().get(x));

    // finish/commit
    agent.finish();
    // read back in a new transaction, must see new value
    result = opex.execute(OperationContext.DEFAULT, new Read(table, a, x));
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

    // read row a outside the transaction, should return nothing
    OperationResult<Map<byte[], byte[]>> result =
        opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());

    // read back row a inside the smart xaction and verify that it sees both x and y
    result = agent.execute(new ReadColumnRange(table, a, null, null));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    Assert.assertArrayEquals(two, result.getValue().get(y));

    // read back in a new transaction, must still see nothing
    result = opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());

    // finish/commit
    agent.finish();
    // read back in a new transaction, must see new values
    result = opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertArrayEquals(one, result.getValue().get(x));
    Assert.assertArrayEquals(two, result.getValue().get(y));
    result = opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, b, null, null));
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
    // and finish the smart xaction
    agent.finish();

    // read back in a new transaction, must see new values
    result = opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, a, null, null));
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

    // try to increment that, should fail
    try {
      agent.execute(new Increment(table, a, x, 1L));
      Assert.fail("Increment should have failed (not compatible)");
    } catch (OperationException e) {
      // expected
    }

    // ensure the write was rolled back
    OperationResult<Map<byte[], byte[]>> result =
      opex.execute(OperationContext.DEFAULT, new Read(table, a, x));
    Assert.assertTrue(result.isEmpty());

    // attempt to abort the agent, should be ok even though agent is already aborted
    agent.abort();
  }

  // start, write, read, write, abort
  @Test
  public void testStartWriteReadWriteAbort() throws OperationException {
    final String table = "tSWF";
    SmartTransactionAgent agent = newAgent();
    agent.start();

    // write a new value
    agent.submit(new Write(table, a, x, one));

    // read this value back in-xaction, must see value
    OperationResult<Map<byte[], byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertArrayEquals(one, result.getValue().get(x));

    // write a new value to different column
    agent.submit(new Write(table, a, y, two));

    // abort the agent
    agent.abort();

    // ensure both writes were rolled back
    // read back in a new transaction, must still see nothing
    result = opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, a, null, null));
    Assert.assertTrue(result.isEmpty());
  }

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

}
