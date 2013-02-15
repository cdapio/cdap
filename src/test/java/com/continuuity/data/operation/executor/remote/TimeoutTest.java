package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.ReadKey;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeoutTest extends OpexServiceTestBase {

  static OperationContext context = OperationContext.DEFAULT;
  static final AtomicInteger clearCount = new AtomicInteger(0);


  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration config = CConfiguration.create();
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_TIMEOUT, 500);
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_LONG_TIMEOUT, 1500);
    config.set(Constants.CFG_DATA_OPEX_CLIENT_RETRY_STRATEGY, "n-times");
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_ATTEMPTS, 3);
    OperationExecutorServiceTest.startService(config,
        new NoOperationExecutor() {
          @Override
          public String getName() {
            return "noop(sleep on write)";
          }

          @Override
          public void execute(OperationContext context,
                              WriteOperation write) throws OperationException {
            if (write instanceof Write) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
            }
            super.execute(context, write);
          }

          @Override
          public void execute(OperationContext context,
                              List<WriteOperation> batch)
              throws OperationException {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            super.execute(context, batch);
          }

          int readCount = 0;

          @Override
          public OperationResult<byte[]> execute(
              OperationContext context, ReadKey read)
              throws OperationException {
            if (++readCount < 3) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
              return super.execute(context, read);
            } else {
              return new OperationResult<byte[]>(
                  new byte[]{(byte) readCount});
            }
          }

          @Override
          public void execute(OperationContext context,
                              ClearFabric clear) {
            clearCount.incrementAndGet();
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            super.execute(context, clear);
          }


        });
  }

  /**
   * This tests that the thrift client times out and returns an error or
   * non-success in some other way.
   */
  @Test(expected = OperationException.class)
  public void testThriftTimeout() throws OperationException {
    Write write = new Write("x".getBytes(), "1".getBytes());
    remote.execute(context, write);
  }

  /**
   * This tests that the thrift client times out and returns an error or
   * non-success in some other way.
   */
  @Test(expected = OperationException.class)
  public void testThriftTimeoutBatch() throws OperationException {
    List<WriteOperation> batch = Lists.newArrayList();
    Write write = new Write("x".getBytes(), "1".getBytes());
    batch.add(write);
    remote.execute(context, batch);
  }

  /**
   * This tests that the thrift client retries (see override of read() above.
   */
  @Test
  public void testRetry() throws OperationException {
    ReadKey read = new ReadKey("x".getBytes());
    Assert.assertArrayEquals(new byte[] { 3 },
        remote.execute(context, read).getValue());
  }

  /**
   * This tests that clear fabric has a longer timeout
   */
  @Test
  public void testLongTimeout() throws OperationException {
    ClearFabric clearFabric = new ClearFabric(ClearFabric.ToClear.ALL);
    remote.execute(context, clearFabric);
    // ensure that no retry was needed
    Assert.assertEquals(1, clearCount.get());
  }

}
