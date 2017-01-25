/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.api.retry.RetryableException;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception thrown when the service is not running.
 */
public class ServiceUnavailableException extends RetryableException implements HttpErrorStatusProvider {
  private final String serviceName;

  public ServiceUnavailableException(String serviceName) {
    super("Service '" + serviceName + "' is not available. Please wait until it is up and running.");
    this.serviceName = serviceName;
  }

  public ServiceUnavailableException(String serviceName, Throwable cause) {
    super("Service '" + serviceName + "' is not available. Please wait until it is up and running.", cause);
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return serviceName;
  }

  @Override
  public int getStatusCode() {
    return HttpResponseStatus.SERVICE_UNAVAILABLE.getCode();
  }
}
