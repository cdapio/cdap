package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.Write;
import com.continuuity.api.data.WriteOperation;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.BatchOperationException;
import com.continuuity.data.operation.executor.BatchOperationResult;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.google.common.collect.Lists;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.SocketException;
import java.util.List;

public class TimeoutTest extends OpexServiceTestBase {

  @BeforeClass
  public static void startService() throws Exception {
    CConfiguration config = CConfiguration.create();
    config.setInt(Constants.CFG_DATA_OPEX_CLIENT_TIMEOUT, 1000);
    OperationExecutorServiceTest.startService(config,
        new NoOperationExecutor() {
          @Override
          public String getName() {
            return "noop(sleep on write)";
          }
          @Override
          public boolean execute(Write write) {
            try {
              Thread.sleep(3000);
            } catch (InterruptedException e) {
              // do nothing
            }
            return super.execute(write);
          }
          @Override
          public BatchOperationResult execute(List<WriteOperation> batch)
              throws BatchOperationException {
            try {
              Thread.sleep(3000);
            } catch (InterruptedException e) {
              // do nothing
            }
            return super.execute(batch);
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
    } catch (BatchOperationException e) {
      // exception should have a message
      Assert.assertNotNull(e.getMessage());
      // cause of exception should be thrift transport exception
      Assert.assertEquals(
          TTransportException.class,
          e.getCause().getClass());
      // cause of cause should be SocketException
      Assert.assertEquals(
          SocketException.class,
          e.getCause().getCause().getClass());
      throw e;
    }
  }
}
