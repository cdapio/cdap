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
    AtomicReference<Throwable> failedThrowable = new AtomicReference<>();
    service.addListener(new Service.Listener() {
      @Override
      public void running() {
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
    service.startAsync();
    try {
      if (!runningLatch.await(timeout, timeoutUnit)) {
        throw new TimeoutException(timeoutErrorMessage);
      }
      if (failedThrowable.get() != null) {
        throw new ExecutionException(failedThrowable.get());
      }
      if (service.state() != Service.State.RUNNING) {
        throw new ExecutionException(new IllegalStateException("Service failed to start"));
      }
    } catch (TimeoutException e) {
      LOG.error(timeoutErrorMessage != null ? timeoutErrorMessage
          : "Timeout while waiting to start service.", e);
      TimeoutException timeoutException = new TimeoutException(timeoutErrorMessage);
      if (e.getStackTrace() != null) {
        timeoutException.setStackTrace(e.getStackTrace());
      }
      try {
        service.stopAsync();
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service: ", stopException);
      }
      throw timeoutException;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to start service.", e);
      try {
        service.stopAsync();
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service:", stopException);
      }
      throw e;
    } catch (ExecutionException e) {
      LOG.error("Failed to start service.", e);
      try {
        service.stopAsync();
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
}
