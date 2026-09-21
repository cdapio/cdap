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

import com.google.common.util.concurrent.Service;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
    try {
      service.startAsync().awaitRunning(timeout, timeoutUnit);
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
    } catch (IllegalStateException e) {
      LOG.error("Failed to start service.", e);
      try {
        service.stopAsync();
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service:", stopException);
      }
      throw new ExecutionException(e);
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
   * Starts a service and waits for it to be running, using reflection
   * to be compatible with both Guava 13 and Guava 15+ / 20+.
   */
  public static void startAndWait(Object service) {
    if (service == null) {
      return;
    }
    try {
      try {
        Method startAndWait = service.getClass().getMethod("startAndWait");
        startAndWait.invoke(service);
        return;
      } catch (NoSuchMethodException e) {
        // Fall back to startAsync
      }
      service.getClass().getMethod("startAsync").invoke(service);
      service.getClass().getMethod("awaitRunning").invoke(service);
    } catch (Throwable t) {
      try {
        // Guava 13 fallback (e.g. for InMemoryZKServer)
        Object future = service.getClass().getMethod("start").invoke(service);
        if (future instanceof Future) {
          ((Future<?>) future).get();
        }
      } catch (Throwable t2) {
        Throwable cause = t.getCause() != null ? t.getCause() : t;
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new RuntimeException(cause);
      }
    }
  }

  /**
   * Stops a service and waits for it to be terminated, using reflection
   * to be compatible with both Guava 13 and Guava 15+ / 20+.
   */
  public static void stopAndWait(Object service) {
    if (service == null) {
      return;
    }
    try {
      try {
        Method stopAndWait = service.getClass().getMethod("stopAndWait");
        stopAndWait.invoke(service);
        return;
      } catch (NoSuchMethodException e) {
        // Fall back to stopAsync
      }
      service.getClass().getMethod("stopAsync").invoke(service);
      service.getClass().getMethod("awaitTerminated").invoke(service);
    } catch (Throwable t) {
      try {
        // Guava 13 fallback (e.g. for InMemoryZKServer)
        Object future = service.getClass().getMethod("stop").invoke(service);
        if (future instanceof Future) {
          ((Future<?>) future).get();
        }
      } catch (Throwable t2) {
        Throwable cause = t.getCause() != null ? t.getCause() : t;
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw new RuntimeException(cause);
      }
    }
  }
}
