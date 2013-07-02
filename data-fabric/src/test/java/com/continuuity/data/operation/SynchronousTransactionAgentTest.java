package com.continuuity.data.operation;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.util.OperationUtil;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class SynchronousTransactionAgentTest {

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

  static TransactionAgent newAgent() throws OperationException {
    TransactionAgent agent = new SynchronousTransactionAgent(opex, OperationUtil.DEFAULT);
    agent.start();
    return agent;
  }

  private static final byte[] a = { 'a' };
  private static final byte[] b = { 'b' };
  private static final byte[] c = { 'c' };
  private static final byte[] x = { 'x' };
  private static final byte[] y = { 'y' };
  private static final byte[] one = Bytes.toBytes(1L);
  private static final byte[] two = Bytes.toBytes(2L);
  private static final byte[] three = Bytes.toBytes(3L);

  // test that all writes and reads go through, and abort has no effect
  @Test
  public void testWritesAndReadsGoThrough() throws OperationException {
    final String table = "tWARGT";

    // start a batch xaction
    TransactionAgent agent = newAgent();

    // write a row and then a batch of two rows through the xaction agent
    agent.submit(new Write(table, c, x, three));
    agent.submit(batch(new Write(table, a, x, one),
                       new Write(table, b, y, two)));
    Assert.assertEquals(3, agent.getSucceededCount());

    // sync read all rows through agent and verify
    Assert.assertArrayEquals(three, agent.execute(new Read(table, c, x)).getValue().get(x));
    Assert.assertArrayEquals(two, agent.execute(new Read(table, b, y)).getValue().get(y));
    Assert.assertArrayEquals(one, agent.execute(new Read(table, a, x)).getValue().get(x));
    Assert.assertEquals(6, agent.getSucceededCount());

    // sync read all rows without agent and verify
    Assert.assertArrayEquals(three, opex.execute(OperationUtil.DEFAULT, new Read(table, c, x)).getValue().get(x));
    Assert.assertArrayEquals(two, opex.execute(OperationUtil.DEFAULT, new Read(table, b, y)).getValue().get(y));
    Assert.assertArrayEquals(one, opex.execute(OperationUtil.DEFAULT, new Read(table, a, x)).getValue().get(x));

    // fail a compare-and-swap and make sure the failed count goes up
    try {
      agent.submit(new CompareAndSwap(table, c, x, x, x));
      Assert.fail("compare-and-swap should have failed");
    } catch (OperationException e) {
      // expected
    }
    Assert.assertEquals(6, agent.getSucceededCount());
    Assert.assertEquals(1, agent.getFailedCount());

    // abort the agent
    agent.abort();
    Assert.assertEquals(6, agent.getSucceededCount());
    Assert.assertEquals(1, agent.getFailedCount());

    // sync read all rows without agent and verify writes are still there
    // sync read all rows without agent and verify
    Assert.assertArrayEquals(three, opex.execute(OperationUtil.DEFAULT, new Read(table, c, x)).getValue().get(x));
    Assert.assertArrayEquals(two, opex.execute(OperationUtil.DEFAULT, new Read(table, b, y)).getValue().get(y));
    Assert.assertArrayEquals(one, opex.execute(OperationUtil.DEFAULT, new Read(table, a, x)).getValue().get(x));
  }

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

}
