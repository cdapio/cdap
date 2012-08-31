package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.ReadKey;
import com.continuuity.api.data.Write;
import com.continuuity.api.data.WriteOperation;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
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
          public boolean execute(Write write) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            return super.execute(write);
          }
          @Override
          public BatchOperationResult execute(List<WriteOperation> batch)
              throws OmidTransactionException {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            return super.execute(batch);
          }
          int readCount = 0;
          @Override
          public byte[] execute(ReadKey read) {
            if (++readCount < 3) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
              return super.execute(read);
            } else {
              return new byte[] { (byte)readCount };
            }
          }
        });
  }

  /**
   * This tests that the thrift client times out and returns an error or
   * non-success in some other way.
   */
  @Test(expected = BatchOperationException.class)
  public void testThriftTimeout() throws BatchOperationException {
    Write write = new Write("x".getBytes(), "1".getBytes());
    Assert.assertFalse(remote.execute(write));

    List<WriteOperation> batch = Lists.newArrayList();
    batch.add(write);
    try {
      remote.execute(batch);
    } catch (OmidTransactionException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Test
  public void testRetry() {
    ReadKey read = new ReadKey("x".getBytes());
    Assert.assertArrayEquals(new byte[] { 3 }, remote.execute(read));
  }
}
