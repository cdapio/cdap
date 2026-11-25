/*
 * Copyright © 2024 Cask Data, Inc.
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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Services}.
 */
public class ServicesTest {

  @Test
  public void testNormalStartup() throws Exception {
    AtomicBoolean started = new AtomicBoolean(false);
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
        started.set(true);
      }

      @Override
      protected void shutDown() {
        started.set(false);
      }
    };

    Services.startAndWait(service, 5, TimeUnit.SECONDS);
    Assert.assertTrue(started.get());
    Assert.assertEquals(Service.State.RUNNING, service.state());
    service.stopAsync().awaitTerminated();
    Assert.assertFalse(started.get());
    Assert.assertEquals(Service.State.TERMINATED, service.state());

    AtomicBoolean secondStarted = new AtomicBoolean(false);
    Service secondService = new AbstractIdleService() {
      @Override
      protected void startUp() {
        secondStarted.set(true);
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };
    Services.startAndWait(secondService, 5, TimeUnit.SECONDS, "Timed out starting second service");
    Assert.assertTrue(secondStarted.get());
    Assert.assertEquals(Service.State.RUNNING, secondService.state());
    secondService.stopAsync().awaitTerminated();
  }

  @Test
  public void testStartAlreadyRunningService() throws Exception {
    AtomicInteger startCount = new AtomicInteger(0);
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
        startCount.incrementAndGet();
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };

    service.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Assert.assertEquals(1, startCount.get());

    Services.startAndWait(service, 5, TimeUnit.SECONDS, "Should not time out on running service");
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Assert.assertEquals(1, startCount.get());

    service.stopAsync().awaitTerminated();
  }

  @Test
  public void testStartupExceptionUnwrapsCause() throws Exception {
    IllegalStateException uncheckedCause = new IllegalStateException("unchecked startup failure");
    Service uncheckedFailingService = new AbstractIdleService() {
      @Override
      protected void startUp() {
        throw uncheckedCause;
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };

    try {
      Services.startAndWait(uncheckedFailingService, 5, TimeUnit.SECONDS, "timeout");
      Assert.fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertSame(uncheckedCause, e.getCause());
    }
    Assert.assertEquals(Service.State.FAILED, uncheckedFailingService.state());

    IOException checkedCause = new IOException("checked startup failure");
    Service checkedFailingService = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        throw checkedCause;
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };

    try {
      Services.startAndWait(checkedFailingService, 5, TimeUnit.SECONDS);
      Assert.fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertSame(checkedCause, e.getCause());
    }
    Assert.assertEquals(Service.State.FAILED, checkedFailingService.state());
  }

  @Test
  public void testStartupTimeoutStopsService() throws Exception {
    AtomicBoolean shutDownCalled = new AtomicBoolean(false);
    Service hungService = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        // Block in STARTING until Services.startAndWait times out and invokes stop().
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (state() == Service.State.STARTING && System.nanoTime() < deadline) {
          TimeUnit.MILLISECONDS.sleep(5);
        }
      }

      @Override
      protected void shutDown() {
        shutDownCalled.set(true);
      }
    };

    String expectedMessage = "Timed out waiting for hung service";
    try {
      Services.startAndWait(hungService, 25, TimeUnit.MILLISECONDS, expectedMessage);
      Assert.fail("Expected TimeoutException");
    } catch (TimeoutException e) {
      Assert.assertEquals(expectedMessage, e.getMessage());
    }
    Tasks.waitFor(Service.State.TERMINATED, hungService::state,
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(shutDownCalled.get());

    // Also verify 3-arg overload (null timeoutErrorMessage).
    Service secondHungService = new AbstractService() {
      @Override
      protected void doStart() {
        // Never calls notifyStarted().
      }

      @Override
      protected void doStop() {
        notifyStopped();
      }
    };

    try {
      Services.startAndWait(secondHungService, 20, TimeUnit.MILLISECONDS);
      Assert.fail("Expected TimeoutException");
    } catch (TimeoutException e) {
      Assert.assertNull(e.getMessage());
    }
    Assert.assertEquals(Service.State.STOPPING, secondHungService.state());
  }

  @Test
  public void testStartupInterruptedStopsService() throws Exception {
    CountDownLatch enteredStartLatch = new CountDownLatch(1);
    AtomicBoolean shutDownCalled = new AtomicBoolean(false);
    Service blockingService = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        enteredStartLatch.countDown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (state() == Service.State.STARTING && System.nanoTime() < deadline) {
          TimeUnit.MILLISECONDS.sleep(5);
        }
      }

      @Override
      protected void shutDown() {
        shutDownCalled.set(true);
      }
    };

    AtomicReference<Throwable> thrownRef = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        Services.startAndWait(blockingService, 10, TimeUnit.SECONDS, "Should be interrupted");
      } catch (Throwable t) {
        thrownRef.set(t);
      }
    });
    caller.start();

    Assert.assertTrue(enteredStartLatch.await(5, TimeUnit.SECONDS));
    caller.interrupt();
    caller.join(5000);
    Assert.assertFalse(caller.isAlive());
    Assert.assertTrue(thrownRef.get() instanceof InterruptedException);
    Tasks.waitFor(Service.State.TERMINATED, blockingService::state,
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(shutDownCalled.get());
  }

  @Test
  public void testShortLivedExecutionThreadService() throws Exception {
    AtomicBoolean startUpCalled = new AtomicBoolean(false);
    AtomicBoolean runCalled = new AtomicBoolean(false);
    AtomicBoolean shutDownCalled = new AtomicBoolean(false);

    Service shortLivedService = new AbstractExecutionThreadService() {
      @Override
      protected void startUp() {
        startUpCalled.set(true);
      }

      @Override
      protected void run() {
        runCalled.set(true);
        // Exit immediately so STARTING -> RUNNING -> STOPPING -> TERMINATED completes right away.
      }

      @Override
      protected void shutDown() {
        shutDownCalled.set(true);
      }
    };

    Services.startAndWait(shortLivedService, 5, TimeUnit.SECONDS, "Short-lived service timed out");
    Assert.assertTrue(startUpCalled.get());
    Tasks.waitFor(Service.State.TERMINATED, shortLivedService::state,
        5, TimeUnit.SECONDS, 10, TimeUnit.MILLISECONDS);
    Assert.assertTrue(runCalled.get());
    Assert.assertTrue(shutDownCalled.get());
  }

  @Test
  public void testStartAndWaitIsIdempotent() {
    AtomicInteger startUpCount = new AtomicInteger();
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
        startUpCount.incrementAndGet();
      }

      @Override
      protected void shutDown() {
      }
    };

    Assert.assertEquals(Service.State.RUNNING, Services.startAndWait(service));
    // Starting a running service again is a no-op, like startAndWait() in Guava 13
    Assert.assertEquals(Service.State.RUNNING, Services.startAndWait(service));
    Assert.assertEquals(1, startUpCount.get());

    Assert.assertEquals(Service.State.TERMINATED, Services.stopAndWait(service));
    // Stopping a terminated service again is a no-op
    Assert.assertEquals(Service.State.TERMINATED, Services.stopAndWait(service));
    // Starting a terminated service does not throw and reports the terminated state
    Assert.assertEquals(Service.State.TERMINATED, Services.startAndWait(service));
    Assert.assertEquals(1, startUpCount.get());
  }

  @Test
  public void testStopAndWaitOnNewService() {
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
      }

      @Override
      protected void shutDown() {
      }
    };
    Assert.assertEquals(Service.State.TERMINATED, Services.stopAndWait(service));
  }

  @Test
  public void testStartAndWaitFailure() {
    RuntimeException failure = new RuntimeException("start failure");
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() {
        throw failure;
      }

      @Override
      protected void shutDown() {
      }
    };

    try {
      Services.startAndWait(service);
      Assert.fail("Expected UncheckedExecutionException");
    } catch (UncheckedExecutionException e) {
      Assert.assertSame(failure, e.getCause());
    }
    // A second call reports the same failure instead of an IllegalStateException
    try {
      Services.startAndWait(service);
      Assert.fail("Expected UncheckedExecutionException");
    } catch (UncheckedExecutionException e) {
      Assert.assertSame(failure, e.getCause());
    }
  }

  @Test
  public void testStartFuture() throws Exception {
    CountDownLatch startLatch = new CountDownLatch(1);
    Service service = new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        startLatch.await();
      }

      @Override
      protected void shutDown() {
      }
    };

    ListenableFuture<Service.State> first = Services.start(service);
    ListenableFuture<Service.State> second = Services.start(service);
    Assert.assertFalse(first.isDone());
    Assert.assertFalse(second.isDone());
    startLatch.countDown();
    Assert.assertEquals(Service.State.RUNNING, first.get(5, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.RUNNING, second.get(5, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, Services.stop(service).get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testStartCallsOverriddenStartAsync() {
    // Reference counted wrappers override startAsync(). Like start() in Guava 13, the helper must
    // call it even if the service is already running.
    AtomicInteger startCalls = new AtomicInteger();
    Service delegate = new AbstractIdleService() {
      @Override
      protected void startUp() {
      }

      @Override
      protected void shutDown() {
      }
    };
    Service service = new Service() {
      @Override
      public Service startAsync() {
        if (startCalls.getAndIncrement() == 0) {
          delegate.startAsync();
        }
        return this;
      }

      @Override
      public boolean isRunning() {
        return delegate.isRunning();
      }

      @Override
      public State state() {
        return delegate.state();
      }

      @Override
      public Service stopAsync() {
        delegate.stopAsync();
        return this;
      }

      @Override
      public void awaitRunning() {
        delegate.awaitRunning();
      }

      @Override
      public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
        delegate.awaitRunning(timeout, unit);
      }

      @Override
      public void awaitTerminated() {
        delegate.awaitTerminated();
      }

      @Override
      public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
        delegate.awaitTerminated(timeout, unit);
      }

      @Override
      public Throwable failureCause() {
        return delegate.failureCause();
      }

      @Override
      public void addListener(Listener listener, Executor executor) {
        delegate.addListener(listener, executor);
      }
    };

    Services.startAndWait(service);
    Services.startAndWait(service);
    Assert.assertEquals(2, startCalls.get());
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Services.stopAndWait(service);
  }
}
