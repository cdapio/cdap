/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling {@link Service} objects.
 */
public class Services {

  private static final Logger LOG = LoggerFactory.getLogger(Services.class);

  private Services() {
  }

  /**
   * Attempts to start the passed in service
   *
   * @param service The service to start
   * @param timeout The duration to wait for the service to start
   * @param timeoutUnit The time unit used for the timeout parameter
   * @param timeoutErrorMessage An optional error message to display if starting the service
   *     times out
   * @throws TimeoutException If the service can not be started before the specified timeout
   * @throws InterruptedException If the service is interrupted while trying to start the
   *     service
   * @throws ExecutionException If an exception occurs while trying to start the service
   */
  public static void startAndWait(Service service, long timeout, TimeUnit timeoutUnit,
      @Nullable String timeoutErrorMessage)
      throws TimeoutException, InterruptedException, ExecutionException {
    CountDownLatch runningLatch = new CountDownLatch(1);
    AtomicBoolean started = new AtomicBoolean(false);
    AtomicReference<Throwable> failedThrowable = new AtomicReference<>();
    service.addListener(new Service.Listener() {
      @Override
      public void running() {
        started.set(true);
        runningLatch.countDown();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        failedThrowable.set(failure);
        runningLatch.countDown();
      }

      @Override
      public void terminated(Service.State from) {
        runningLatch.countDown();
      }
    }, MoreExecutors.directExecutor());
    try {
      Service.State currentState = service.state();
      if (currentState == Service.State.RUNNING) {
        return;
      }
      if (currentState == Service.State.FAILED) {
        throw new ExecutionException(service.failureCause());
      }
      if (currentState == Service.State.NEW) {
        try {
          service.startAsync();
        } catch (IllegalStateException e) {
          Service.State racedState = service.state();
          if (racedState == Service.State.RUNNING) {
            return;
          }
          if (racedState != Service.State.STARTING && !started.get()) {
            throw e;
          }
        }
      } else if (currentState != Service.State.STARTING && !started.get()) {
        throw new ExecutionException(
            new IllegalStateException("Service cannot be started from state " + currentState));
      }
      if (service.state() == Service.State.RUNNING || started.get()) {
        return;
      }
      if (service.state() == Service.State.FAILED) {
        throw new ExecutionException(service.failureCause());
      }
      if (!runningLatch.await(timeout, timeoutUnit)) {
        throw new TimeoutException(timeoutErrorMessage);
      }
      if (failedThrowable.get() != null) {
        throw new ExecutionException(failedThrowable.get());
      }
      if (!started.get() && service.state() != Service.State.RUNNING) {
        throw new ExecutionException(new IllegalStateException("Service failed to start"));
      }
    } catch (TimeoutException e) {
      LOG.error(timeoutErrorMessage != null ? timeoutErrorMessage
          : "Timeout while waiting to start service.", e);
      TimeoutException timeoutException = new TimeoutException(timeoutErrorMessage);
      if (e.getStackTrace() != null) {
        timeoutException.setStackTrace(e.getStackTrace());
      }
      stopQuietly(service);
      throw timeoutException;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to start service.", e);
      stopQuietly(service);
      throw e;
    } catch (IllegalStateException e) {
      LOG.error("Failed to start service.", e);
      stopQuietly(service);
      throw new ExecutionException(e);
    } catch (ExecutionException e) {
      LOG.error("Failed to start service.", e);
      stopQuietly(service);
      throw e;
    }
  }

  private static void stopQuietly(Service service) {
    try {
      service.stopAsync();
    } catch (Exception stopException) {
      LOG.error("Error while trying to stop service:", stopException);
    }
  }

  /**
   * Starts {@code service} if it is currently in {@link Service.State#NEW}, ignoring benign races
   * when another caller starts the same shared service instance concurrently.
   *
   * @param service the service to start
   */
  public static void startIfNew(Service service) {
    if (service.state() == Service.State.NEW) {
      try {
        service.startAsync();
      } catch (IllegalStateException e) {
        Service.State racedState = service.state();
        if (racedState != Service.State.STARTING && racedState != Service.State.RUNNING) {
          throw e;
        }
      }
    }
  }

  /**
   * Starts {@code service} if it is in {@link Service.State#NEW} and awaits {@code RUNNING}.
   *
   * @param service the service to start and await
   */
  public static void startAndAwaitRunning(Service service) {
    startIfNew(service);
    service.awaitRunning();
  }

  /**
   * Sequentially stops each non-null {@link Service} and waits for termination, logging any
   * exception so remaining services still stop cleanly.
   *
   * @param services services to stop in order
   */
  public static void stopAllQuietly(@Nullable Service... services) {
    if (services == null) {
      return;
    }
    for (Service service : services) {
      if (service == null) {
        continue;
      }
      try {
        service.stopAsync().awaitTerminated();
      } catch (Exception e) {
        LOG.warn("Error while stopping service {}", service, e);
      }
    }
  }

  /**
   * See {@link Services#startAndWait(Service, long, TimeUnit, String)}
   */
  public static void startAndWait(Service service, long timeout, TimeUnit timeoutUnit)
      throws TimeoutException, InterruptedException, ExecutionException {
    startAndWait(service, timeout, timeoutUnit, null);
  }
}
