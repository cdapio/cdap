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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    ListenableFuture<Service.State> startFuture = start(service);
    try {
      startFuture.get(timeout, timeoutUnit);
    } catch (TimeoutException e) {
      LOG.error(timeoutErrorMessage != null ? timeoutErrorMessage
          : "Timeout while waiting to start service.", e);
      TimeoutException timeoutException = new TimeoutException(timeoutErrorMessage);
      if (e.getStackTrace() != null) {
        timeoutException.setStackTrace(e.getStackTrace());
      }
      try {
        stop(service);
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service: ", stopException);
      }
      throw timeoutException;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to start service.", e);
      try {
        stop(service);
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service:", stopException);
      }
      throw e;
    }
  }

  /**
   * See {@link Services#startAndWait(Service, long, TimeUnit, String)}
   */
  public static void startAndWait(Service service, long timeout, TimeUnit timeoutUnit)
      throws TimeoutException, InterruptedException, ExecutionException {
    startAndWait(service, timeout, timeoutUnit, null);
  }

  /**
   * Starts the service and waits for it to be running, with the same behavior as
   * {@code Service.startAndWait()} in Guava 13, which later Guava releases removed. It does not
   * throw if the service is already starting or running.
   *
   * @return the state of the service after the start completes
   * @throws com.google.common.util.concurrent.UncheckedExecutionException if the service fails
   */
  public static Service.State startAndWait(Service service) {
    return Futures.getUnchecked(start(service));
  }

  /**
   * Stops the service and waits for it to terminate, with the same behavior as
   * {@code Service.stopAndWait()} in Guava 13, which later Guava releases removed.
   *
   * @return the state of the service after the stop completes
   * @throws com.google.common.util.concurrent.UncheckedExecutionException if the service fails
   */
  public static Service.State stopAndWait(Service service) {
    return Futures.getUnchecked(stop(service));
  }

  /**
   * Starts the service if it is new and returns a future for the start, with the same behavior as
   * {@code Service.start()} in Guava 13, which later Guava releases removed. Calling it on a
   * service that is already starting or running does not throw. The future completes with
   * {@link Service.State#RUNNING} once the service is running, fails with the failure cause if the
   * service fails, or completes with {@link Service.State#TERMINATED} if the service terminates
   * first.
   */
  public static ListenableFuture<Service.State> start(Service service) {
    SettableFuture<Service.State> future = SettableFuture.create();
    service.addListener(new Service.Listener() {
      @Override
      public void running() {
        future.set(Service.State.RUNNING);
      }

      @Override
      public void terminated(Service.State from) {
        future.set(Service.State.TERMINATED);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.setException(failure);
      }
    }, MoreExecutors.directExecutor());

    try {
      // Always call startAsync() so that subclasses overriding it (for example reference counted
      // wrappers) are invoked, just like start() was in Guava 13.
      service.startAsync();
    } catch (IllegalStateException e) {
      // The service is not new. The state check below completes the future.
    }
    completeFromState(service, future, Service.State.RUNNING);
    return future;
  }

  /**
   * Stops the service and returns a future for the stop, with the same behavior as
   * {@code Service.stop()} in Guava 13, which later Guava releases removed. The future completes
   * with {@link Service.State#TERMINATED} once the service is terminated, or fails with the failure
   * cause if the service fails.
   */
  public static ListenableFuture<Service.State> stop(Service service) {
    SettableFuture<Service.State> future = SettableFuture.create();
    service.addListener(new Service.Listener() {
      @Override
      public void terminated(Service.State from) {
        future.set(Service.State.TERMINATED);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.setException(failure);
      }
    }, MoreExecutors.directExecutor());

    service.stopAsync();
    completeFromState(service, future, Service.State.TERMINATED);
    return future;
  }

  /**
   * Completes the future if the service already reached a final state, or the target state, before
   * the listener was added. Listeners added later are not told about earlier transitions.
   */
  private static void completeFromState(Service service, SettableFuture<Service.State> future,
      Service.State target) {
    Service.State state = service.state();
    if (state == Service.State.FAILED) {
      future.setException(service.failureCause());
    } else if (state == Service.State.TERMINATED || state == target) {
      future.set(state);
    }
  }
}
