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

package io.cdap.cdap.common;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.retry.RetryableException;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception thrown when the service is not running.
 */
public class ServiceUnavailableException extends RetryableException implements
    HttpErrorStatusProvider {

  private static final String MESSAGE_FORMAT =
      "Service %s is not available. Please wait until it is up and running.";
  private final String serviceName;

  /**
   * Constructs the {@link ServiceUnavailableException} with the provided service name.
   */
  public ServiceUnavailableException(String serviceName) {
    this(serviceName, String.format(MESSAGE_FORMAT, serviceName));
  }

  /**
   * Constructs the {@link ServiceUnavailableException} with the provided service name and message.
   */
  public ServiceUnavailableException(String serviceName, String message) {
    super(message);
    this.serviceName = serviceName;
  }

  /**
   * Constructs the {@link ServiceUnavailableException} with the provided params.
   */
  public ServiceUnavailableException(String serviceName, Throwable cause) {
    this(serviceName, String.format(MESSAGE_FORMAT, serviceName), cause);
  }

  /**
   * Constructs the {@link ServiceUnavailableException} with the provided params.
   */
  public ServiceUnavailableException(String serviceName, String message, Throwable cause) {
    super(message, cause);
    this.serviceName = serviceName;
  }

  /**
   * Returns the service name.
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Returns the http status code.
   */
  @Override
  public int getStatusCode() {
    return HttpResponseStatus.SERVICE_UNAVAILABLE.code();
  }
}
