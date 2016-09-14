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

package co.cask.cdap.common.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Utility class for handling {@link Service} objects.
 */
public class Services {

  private static final Logger LOG = LoggerFactory.getLogger(Services.class);

  private Services(){}

  /**
   * Attempts to start the passed in service
   * @param service The service to start
   * @param timeout The duration to wait for the service to start
   * @param timeoutUnit The time unit used for the timeout parameter
   * @param timeoutErrorMessage An optional error message to display if starting the service times out
   * @throws TimeoutException If the service can not be started before the specified timeout
   * @throws InterruptedException If the service is interrupted while trying to start the service
   * @throws ExecutionException If an exception occurs while trying to start the service
   */
  public static void startAndWait(Service service, long timeout, TimeUnit timeoutUnit,
                                  @Nullable String timeoutErrorMessage)
    throws TimeoutException, InterruptedException, ExecutionException {
    ListenableFuture<Service.State> startFuture = service.start();
    try {
      startFuture.get(timeout, timeoutUnit);
    } catch (TimeoutException e) {
      LOG.error(timeoutErrorMessage != null ? timeoutErrorMessage : "Timeout while waiting to start service.", e);
      TimeoutException timeoutException = new TimeoutException(timeoutErrorMessage);
      if (e.getStackTrace() != null) {
        timeoutException.setStackTrace(e.getStackTrace());
      }
      try {
        service.stop();
      } catch (Exception stopException) {
        LOG.error("Error while trying to stop service: ", stopException);
      }
      throw timeoutException;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting to start service.", e);
      try {
        service.stop();
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
