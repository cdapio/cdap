/*
 * Copyright © 2018-2024 Cask Data, Inc.
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
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.retry.RetriesExhaustedException;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test class for the {@link AbstractRetryableScheduledService}.
 */
public class RetryableScheduledServiceTest {

  @Test
  public void testNormalIteration() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(5);

    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected long runTask() {
        latch.countDown();
        return 1L;
      }
    };

    service.startAsync();
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    service.stopAsync().awaitTerminated();
  }

  @Test
  public void testRetryExhausted()
      throws InterruptedException, ExecutionException, TimeoutException {
    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected long runTask() throws Exception {
        throw new Exception("Task failed");
      }
    };

    service.startAsync();
    // Wait for the service to fail
    Tasks.waitFor(Service.State.FAILED, service::state, 5, TimeUnit.SECONDS,
        10, TimeUnit.MILLISECONDS);

    try {
      service.stopAsync().awaitTerminated();
      Assert.fail("Expected exception on stopAndWait");
    } catch (Exception e) {
      // The root cause should be the one thrown from runTask, suppressing RetriesExhaustedException.
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertEquals("Task failed", rootCause.getMessage());

      Throwable suppressed = Stream.of(rootCause.getSuppressed())
          .findFirst()
          .orElseThrow(IllegalStateException::new);
      Assert.assertTrue(suppressed instanceof RetriesExhaustedException);
    }
  }

  @Test
  public void testNoRetry() throws InterruptedException, ExecutionException, TimeoutException {
    Service service = new AbstractRetryableScheduledService(
        RetryStrategies.fixDelay(10L, TimeUnit.MILLISECONDS)) {
      @Override
      protected long runTask() throws Exception {
        throw new Exception("Task failed");
      }

      @Override
      protected boolean shouldRetry(Exception ex) {
        return false;
      }
    };

    service.startAsync();
    // Wait for the service to fail
    Tasks.waitFor(Service.State.FAILED, service::state, 5, TimeUnit.SECONDS,
        10, TimeUnit.MILLISECONDS);

    try {
      service.stopAsync().awaitTerminated();
      Assert.fail("Expected exception on stopAndWait");
    } catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      Assert.assertEquals("Task failed", rootCause.getMessage());
    }
  }

  @Test
  public void testFailureRetry() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(3);
    Service service = new AbstractRetryableScheduledService(
        RetryStrategies.fixDelay(1L, TimeUnit.MILLISECONDS)) {

      private int failureCount = 5;

      @Override
      protected long runTask() throws Exception {
        if (--failureCount % 2 == 0) {
          throw new Exception("Task failed");
        }
        latch.countDown();
        return 1L;
      }
    };
    service.startAsync();
    Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
    service.stopAsync().awaitTerminated();
  }

  @Test
  public void testNamedServiceLifecycleAndDaemonThread() throws Exception {
    RetryStrategy strategy = RetryStrategies.noRetry();
    NamedScheduledService service = new NamedScheduledService(strategy);
    Assert.assertSame(strategy, service.exposedRetryStrategy());

    service.startAsync().awaitRunning();
    Assert.assertTrue(service.taskLatch.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(service.startedUp.get());
    Assert.assertTrue(service.threadName.get().startsWith("NamedScheduledService"));
    Assert.assertTrue(service.isDaemon.get());

    service.stopAsync().awaitTerminated();
    Assert.assertTrue(service.shutDownCalled.get());
    Assert.assertEquals(Service.State.TERMINATED, service.state());
  }

  @Test
  public void testHandleRetriesExhaustedRecovery() throws Exception {
    AtomicInteger attempts = new AtomicInteger(0);
    AtomicInteger exhaustedCalls = new AtomicInteger(0);
    CountDownLatch successLatch = new CountDownLatch(2);

    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected long runTask() throws Exception {
        if (attempts.incrementAndGet() <= 2) {
          throw new IOException("transient failure");
        }
        successLatch.countDown();
        return 1L;
      }

      @Override
      protected long handleRetriesExhausted(Exception e) {
        exhaustedCalls.incrementAndGet();
        return 1L;
      }
    };

    service.startAsync().awaitRunning();
    Assert.assertTrue(successLatch.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(2, exhaustedCalls.get());
    service.stopAsync().awaitTerminated();
  }

  @Test
  public void testDoStartUpFailureTransitionsToFailed() throws Exception {
    Service service = new AbstractRetryableScheduledService(RetryStrategies.noRetry()) {
      @Override
      protected void doStartUp() throws Exception {
        super.doStartUp();
        throw new IOException("startup boom");
      }

      @Override
      protected long runTask() {
        return 1L;
      }
    };

    service.startAsync();
    Tasks.waitFor(Service.State.FAILED, service::state, 5, TimeUnit.SECONDS,
        10, TimeUnit.MILLISECONDS);
    try {
      service.stopAsync().awaitTerminated();
      Assert.fail("Expected exception from startup failure");
    } catch (Exception e) {
      Assert.assertEquals("startup boom", Throwables.getRootCause(e).getMessage());
    }
  }

  private static final class NamedScheduledService extends AbstractRetryableScheduledService {
    private final AtomicBoolean startedUp = new AtomicBoolean(false);
    private final AtomicBoolean shutDownCalled = new AtomicBoolean(false);
    private final AtomicReference<String> threadName = new AtomicReference<>();
    private final AtomicBoolean isDaemon = new AtomicBoolean(false);
    private final CountDownLatch taskLatch = new CountDownLatch(1);

    NamedScheduledService(RetryStrategy retryStrategy) {
      super(retryStrategy);
    }

    RetryStrategy exposedRetryStrategy() {
      return getRetryStrategy();
    }

    @Override
    protected void doStartUp() throws Exception {
      super.doStartUp();
      startedUp.set(true);
    }

    @Override
    protected void doShutdown() throws Exception {
      super.doShutdown();
      shutDownCalled.set(true);
    }

    @Override
    protected long runTask() {
      Thread current = Thread.currentThread();
      threadName.set(current.getName());
      isDaemon.set(current.isDaemon());
      taskLatch.countDown();
      return 5L;
    }
  }
}
