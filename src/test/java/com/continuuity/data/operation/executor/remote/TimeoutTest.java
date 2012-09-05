package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class TimeoutTest extends OpexServiceTestBase {

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration config = CConfiguration.create();
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_TIMEOUT, 500);
    config.set(Constants.CFG_DATA_OPEX_CLIENT_RETRY_STRATEGY, "n-times");
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_ATTEMPTS, 3);
    OperationExecutorServiceTest.startService(config,
        new NoOperationExecutor() {
          @Override
          public String getName() {
            return "noop(sleep on write)";
          }
          @Override
          public void execute(Write write) throws OperationException {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            super.execute(write);
          }
          @Override
          public void execute(List<WriteOperation> batch)
              throws OperationException {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            super.execute(batch);
          }
          int readCount = 0;
          @Override
          public OperationResult<byte[]> execute(ReadKey read)
              throws OperationException {
            if (++readCount < 3) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
              return super.execute(read);
            } else {
              return new OperationResult<byte[]>(
                  new byte[] { (byte)readCount });
            }
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
    remote.execute(write);
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
    remote.execute(batch);
  }

  /**
   * This tests that the thrift client retries (see override of read() above.
   */
  @Test
  public void testRetry() throws OperationException {
    ReadKey read = new ReadKey("x".getBytes());
    Assert.assertArrayEquals(new byte[] { 3 }, remote.execute(read).getValue());
  }
}
