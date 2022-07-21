/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.proto;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;

/**
 * An exception that contains an HTTP error code.
 */
public class CodedException extends RuntimeException implements HttpErrorStatusProvider {
  private final int code;

  public CodedException(int code, String message) {
    super(message);
    this.code = code;
  }

  public CodedException(int code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  /**
   *
   * @return HTTP error code
   * @deprecated use {@link #getStatusCode()} instead
   */
  @Deprecated
  public int getCode() {
    return code;
  }

  @Override
  public int getStatusCode() {
    return code;
  }
}
