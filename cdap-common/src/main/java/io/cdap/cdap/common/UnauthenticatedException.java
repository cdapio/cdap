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

package co.cask.cdap.common;

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Thrown when a user is not authenticated.
 */
public class UnauthenticatedException extends Exception implements HttpErrorStatusProvider {

  public UnauthenticatedException() {
    super();
  }

  public UnauthenticatedException(String msg, Throwable throwable) {
    super(msg, throwable);
  }

  public UnauthenticatedException(String message) {
    super(message);
  }

  @Override
  public int getStatusCode() {
    return HttpResponseStatus.UNAUTHORIZED.code();
  }
}
