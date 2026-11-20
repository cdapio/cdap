/*
 * Copyright © 2015-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.service;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RetryOnStartFailureService}.
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
    service.stopAndWait();
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
    CountDownLatch failureLatch = new CountDownLatch(1);
    Service service = new RetryOnStartFailureService(
        createServiceSupplier(1000, new CountDownLatch(1), failureLatch, false),
        RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));
    service.startAndWait();
    Assert.assertTrue(failureLatch.await(1, TimeUnit.SECONDS));
    service.stopAndWait();
  }

  @Test
  public void testStopFailurePropagate()
      throws InterruptedException, TimeoutException, ExecutionException {
    CountDownLatch startLatch = new CountDownLatch(1);
    final RetryOnStartFailureService service = new RetryOnStartFailureService(
        createServiceSupplier(0, startLatch, new CountDownLatch(1), true),
        RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));
    service.startAndWait();
    Assert.assertTrue(startLatch.await(1, TimeUnit.SECONDS));
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return service.getStartedService() != null;
      }
    }, 5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    try {
      service.stopAndWait();
      Assert.fail("Expected failure in stopping");
    } catch (Exception e) {
      Assert.assertEquals("Intentional failure to shutdown",
          Throwables.getRootCause(e).getMessage());
    }
  }

  @Test
  public void testStopWhileDelegateBlockedInStartUp() throws Exception {
    CountDownLatch enteredStartLatch = new CountDownLatch(1);
    AtomicBoolean delegateShutDown = new AtomicBoolean(false);

    RetryOnStartFailureService service = new RetryOnStartFailureService(
        () -> new AbstractIdleService() {
          @Override
          protected void startUp() throws Exception {
            enteredStartLatch.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (state() == State.STARTING && System.nanoTime() < deadline) {
              TimeUnit.MILLISECONDS.sleep(5);
            }
          }

          @Override
          protected void shutDown() {
            delegateShutDown.set(true);
          }
        }, RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));

    service.startAndWait();
    Assert.assertTrue(enteredStartLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals("EndureService{RetryOnStartFailureServiceTest}", service.toString());
    Assert.assertNull(service.getStartedService());

    service.stopAndWait();
    Assert.assertTrue(delegateShutDown.get());
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }

  @Test
  public void testStopWhileUnstartedDelegateFailsInStartUp() throws Exception {
    CountDownLatch enteredStartLatch = new CountDownLatch(1);
    RetryOnStartFailureService service = new RetryOnStartFailureService(
        () -> new AbstractIdleService() {
          @Override
          protected void startUp() throws Exception {
            enteredStartLatch.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (state() == State.STARTING && System.nanoTime() < deadline) {
              TimeUnit.MILLISECONDS.sleep(5);
            }
            throw new IOException("aborted during startUp");
          }

          @Override
          protected void shutDown() {
            // no-op
          }
        }, RetryStrategies.fixDelay(10, TimeUnit.MILLISECONDS));

    service.startAndWait();
    Assert.assertTrue(enteredStartLatch.await(5, TimeUnit.SECONDS));
    // Unstarted delegate failure should not fail RetryOnStartFailureService when stopped.
    service.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }

  @Test
  public void testFastCompletingExecutionThreadDelegate() throws Exception {
    AtomicInteger supplierCount = new AtomicInteger(0);
    AtomicReference<Service> delegateRef = new AtomicReference<>();

    RetryOnStartFailureService service = new RetryOnStartFailureService(() -> {
      supplierCount.incrementAndGet();
      Service fastService = new FastExecutionThreadService();
      delegateRef.set(fastService);
      return fastService;
    }, RetryStrategies.noRetry());

    Assert.assertEquals("EndureService{FastExecutionThreadService}", service.toString());
    service.startAndWait();

    Tasks.waitFor(true, () -> service.getStartedService() != null,
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Tasks.waitFor(Service.State.TERMINATED, () -> delegateRef.get().state(),
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, supplierCount.get());

    // Stopping when startedService has already terminated should succeed cleanly.
    service.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }

  @Test
  public void testStopWhenStartedServiceAlreadyFailed() throws Exception {
    CountDownLatch failNowLatch = new CountDownLatch(1);
    AtomicReference<Service> delegateRef = new AtomicReference<>();

    RetryOnStartFailureService service = new RetryOnStartFailureService(() -> {
      Service runtimeFailingService = new AbstractExecutionThreadService() {
        @Override
        protected void run() throws Exception {
          failNowLatch.await();
          throw new IllegalStateException("delegate failed while running");
        }
      };
      delegateRef.set(runtimeFailingService);
      return runtimeFailingService;
    }, RetryStrategies.noRetry());

    service.startAndWait();
    Tasks.waitFor(true, () -> service.getStartedService() != null,
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    failNowLatch.countDown();
    Tasks.waitFor(Service.State.FAILED, () -> delegateRef.get().state(),
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);

    try {
      service.stopAndWait();
      Assert.fail("Expected stopAndWait to propagate delegate failure");
    } catch (Exception e) {
      Assert.assertEquals("delegate failed while running",
          Throwables.getRootCause(e).getMessage());
    }
    Assert.assertEquals(Service.State.FAILED, service.state());
  }

  private static final class FastExecutionThreadService extends AbstractExecutionThreadService {
    @Override
    protected void run() {
      // Complete immediately after transitioning to RUNNING.
    }
  }

  private Supplier<Service> createServiceSupplier(final int startFailures,
      final CountDownLatch startLatch,
      final CountDownLatch failureLatch,
      final boolean failureOnStop) {
    return new Supplier<Service>() {

      private int failures;

      @Override
      public Service get() {
        return new AbstractIdleService() {
          @Override
          protected void startUp() {
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
