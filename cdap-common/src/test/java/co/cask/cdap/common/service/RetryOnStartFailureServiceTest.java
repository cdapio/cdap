/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RetryOnStartFailureServiceTest {

  @Test
  public void testRetrySucceed() throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    Service service = new RetryOnStartFailureService(
      createServiceSupplier(3, startLatch, new CountDownLatch(1), false),
      RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));
    service.startAndWait();
    Assert.assertTrue(startLatch.await(1, TimeUnit.SECONDS));
  }

  @Test
  public void testRetryFail() throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    Service service = new RetryOnStartFailureService(
      createServiceSupplier(1000, startLatch, new CountDownLatch(1), false),
      RetryStrategies.limit(10, RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS)));

    final CountDownLatch failureLatch = new CountDownLatch(1);
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void failed(Service.State from, Throwable failure) {
        failureLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    service.start();
    Assert.assertTrue(failureLatch.await(1, TimeUnit.SECONDS));
    Assert.assertFalse(startLatch.await(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testStopWhileRetrying() throws InterruptedException {
    // This test the service can be stopped during failure retry
    CountDownLatch failureLatch = new CountDownLatch(1);
    Service service = new RetryOnStartFailureService(
      createServiceSupplier(1000, new CountDownLatch(1), failureLatch, false),
      RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));
    service.startAndWait();
    Assert.assertTrue(failureLatch.await(1, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  @Test
  public void testStopFailurePropagate() throws InterruptedException {
    // This test the underlying service stop state is propagated if the start was successful
    CountDownLatch startLatch = new CountDownLatch(1);
    Service service = new RetryOnStartFailureService(
      createServiceSupplier(0, startLatch, new CountDownLatch(1), true),
      RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));
    service.startAndWait();
    // block until the underlying service started successfully
    Assert.assertTrue(startLatch.await(1, TimeUnit.SECONDS));
    try {
      service.stopAndWait();
      Assert.fail("Expected failure in stopping");
    } catch (Exception e) {
      Assert.assertEquals("Intentional failure to shutdown", Throwables.getRootCause(e).getMessage());
    }
  }

  /**
   * Creates a {@link Supplier} of {@link Service} that the start() call will fail for the first {@code startFailures}
   * instances that it returns.
   */
  private Supplier<Service> createServiceSupplier(final int startFailures,
                                                  final CountDownLatch startLatch,
                                                  final CountDownLatch failureLatch,
                                                  final boolean failureOnStop) {
    return new Supplier<Service>() {

      private int failures = 0;

      @Override
      public Service get() {
        return new AbstractIdleService() {
          @Override
          protected void startUp() throws Exception {
            if (failures++ < startFailures) {
              failureLatch.countDown();
              throw new RuntimeException("Fail");
            }
            startLatch.countDown();
          }

          @Override
          protected void shutDown() throws Exception {
            if (failureOnStop) {
              throw new Exception("Intentional failure to shutdown");
            }
          }
        };
      }
    };
  }
}
