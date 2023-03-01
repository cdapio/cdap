/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authentication;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.security.AccessException;
import java.net.HttpURLConnection;

/**
 * Thrown when a user is not authenticated.
 */
public class UnauthenticatedException extends AccessException implements HttpErrorStatusProvider {

  public UnauthenticatedException() {
    super();
  }

  public UnauthenticatedException(Throwable throwable) {
    super(throwable);
  }

  public UnauthenticatedException(String msg, Throwable throwable) {
    super(msg, throwable);
  }

  public UnauthenticatedException(String message) {
    super(message);
  }

  @Override
  public int getStatusCode() {
    return HttpURLConnection.HTTP_UNAUTHORIZED;
  }
}
