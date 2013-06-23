package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.*;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.util.OperationUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeoutTest extends OpexServiceTestBase {

  static OperationContext context = OperationUtil.DEFAULT;
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
          public void commit(OperationContext context, WriteOperation write) throws OperationException {
            if (write instanceof Write) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
            }
            super.commit(context, write);
          }

          @Override
          public void commit(OperationContext context, List<WriteOperation> batch)
              throws OperationException {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              // do nothing
            }
            super.commit(context, batch);
          }

          int readCount = 0;

          @Override
          public OperationResult<Map<byte[], byte[]>> execute(
              OperationContext context, Read read)
              throws OperationException {
            if (++readCount < 3) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                // do nothing
              }
              return super.execute(context, read);
            } else {
              Map<byte[], byte[]> map = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
              map.put(Operation.KV_COL, new byte[]{(byte) readCount});
              return new OperationResult<Map<byte[], byte[]>>(map);
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
  @Test(expected = OperationException.class, timeout = 10000)
  public void testThriftTimeout() throws OperationException {
    Write write = new Write("x".getBytes(), "c".getBytes(), "1".getBytes());
    remote.commit(context, write);
  }

  /**
   * This tests that the thrift client times out and returns an error or
   * non-success in some other way.
   */
  @Test(expected = OperationException.class, timeout = 10000)
  public void testThriftTimeoutBatch() throws OperationException {
    List<WriteOperation> batch = Lists.newArrayList();
    Write write = new Write("x".getBytes(), "c".getBytes(), "1".getBytes());
    batch.add(write);
    remote.commit(context, batch);
  }

  /**
   * This tests that the thrift client retries (see override of read() above.
   */
  @Test(timeout = 10000)
  public void testRetry() throws OperationException {
    Read read = new Read("x".getBytes(), Operation.KV_COL);
    Assert.assertArrayEquals(new byte[] { 3 },
        remote.execute(context, read).getValue().get(Operation.KV_COL));
  }

  /**
   * This tests that clear fabric has a longer timeout
   */
  @Test(timeout = 10000)
  public void testLongTimeout() throws OperationException {
    ClearFabric clearFabric = new ClearFabric(ClearFabric.ToClear.ALL);
    remote.execute(context, clearFabric);
    // ensure that no retry was needed
    Assert.assertEquals(1, clearCount.get());
  }

}
