/*
 * Copyright © 2017 Cask Data, Inc.
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
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.annotation.Nullable;

/**
 * Similar to Exception, but responds with a custom error code.
 */
public class ServiceException extends RuntimeException implements HttpErrorStatusProvider {

  private final HttpResponseStatus status;
  private final String jsonDetails;

  public ServiceException(String message, @Nullable Throwable cause, HttpResponseStatus status) {
    this(message, cause, null, status);
  }

  public ServiceException(@Nullable Throwable cause, HttpResponseStatus status) {
    this(cause, null, status);
  }

  public ServiceException(String message, @Nullable Throwable cause, @Nullable String jsonDetails,
      HttpResponseStatus status) {
    super(message, cause);
    this.status = status;
    this.jsonDetails = jsonDetails;
  }

  public ServiceException(@Nullable Throwable cause, @Nullable String jsonDetails,
      HttpResponseStatus status) {
    super(cause);
    this.status = status;
    this.jsonDetails = jsonDetails;
  }

  @Override
  public int getStatusCode() {
    return status.code();
  }

  @Nullable
  public String getJsonDetails() {
    return jsonDetails;
  }
}
