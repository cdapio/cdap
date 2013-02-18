package com.continuuity.data.operation;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.executor.BatchTransactionAgentWithSyncReads;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BatchTransactionAgentTest {

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

  static TransactionAgent newAgent() throws OperationException {
    TransactionAgent agent = new BatchTransactionAgentWithSyncReads(opex, OperationContext.DEFAULT);
    agent.start();
    return agent;
  }

  static final byte[] a = { 'a' };
  static final byte[] b = { 'b' };
  static final byte[] c = { 'c' };
  static final byte[] x = { 'x' };
  static final byte[] y = { 'y' };
  static final byte[] one = Bytes.toBytes(1L);
  static final byte[] two = Bytes.toBytes(2L);
  static final byte[] three = Bytes.toBytes(3L);
  static final byte[] four = Bytes.toBytes(4L);

  // test that writes are deferred but reads go through
  @Test
  public void testWritesAreDeferredButNotReads() throws OperationException {
    final String table = "tWADBNR";

    // write two rows outside the xaction
    opex.commit(OperationContext.DEFAULT, batch(new Write(table, a, x, one), new Write(table, b, y, two)));
    // start a batch xaction
    TransactionAgent agent = newAgent();

    // sync read one row
    OperationResult<Map<byte[],byte[]>> result = agent.execute(new Read(table, a, x));
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(one, result.getValue().get(x));

    // sync increment the other row
    OperationResult<Map<byte[],Long>> incr = agent.execute(new Increment(table, b, y, 1L));
    Assert.assertFalse(incr.isEmpty());
    Assert.assertEquals((Long)3L, incr.getValue().get(y));

    // add a row with the xaction agent
    agent.submit(new Write(table, c, x, y));
    // batch write the two rows with the xaction agent
    agent.submit(batch(new Write(table, a, x, x), new Increment(table, b, y, 1L)));

    // read and verify old values
    Assert.assertTrue(agent.execute(new ReadColumnRange(table, c, null, null)).isEmpty());
    Assert.assertArrayEquals(three, agent.execute(new Read(table, b, y)).getValue().get(y));
    Assert.assertArrayEquals(one, agent.execute(new Read(table, a, x)).getValue().get(x));

    // finish xaction
    agent.finish();

    // read and verify new values
    Assert.assertArrayEquals(y, opex.execute(OperationContext.DEFAULT, new Read(table, c, x)).getValue().get(x));
    Assert.assertArrayEquals(four, opex.execute(OperationContext.DEFAULT, new Read(table, b, y)).getValue().get(y));
    Assert.assertArrayEquals(x, opex.execute(OperationContext.DEFAULT, new Read(table, a, x)).getValue().get(x));
  }

  // test that writes are not executed after abort
  @Test
  public void testWritesAreNotDoneWhenAborted() throws OperationException {
    final String table = "tWANDWA";

    // write a row outside xaction
    opex.commit(OperationContext.DEFAULT, new Write(table, a, x, one));

    // start batch xaction
    TransactionAgent agent = newAgent();

    // submit add second row
    agent.submit(new Write(table, x, y, two));
    // batch submit delete for first row and add swap for second row
    agent.submit(batch(new Delete(table, a, x), new CompareAndSwap(table, x, y, two, three)));

    // abort xaction
    agent.abort();

    // verify no rows changed
    Assert.assertTrue(opex.execute(OperationContext.DEFAULT, new ReadColumnRange(table, x, null, null)).isEmpty());
    Assert.assertArrayEquals(one, opex.execute(OperationContext.DEFAULT, new Read(table, a, x)).getValue().get(x));
  }

  private static List<WriteOperation> batch(WriteOperation ... ops) {
    return Arrays.asList(ops);
  }

}
