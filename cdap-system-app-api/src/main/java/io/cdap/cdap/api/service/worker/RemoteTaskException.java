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

package io.cdap.cdap.api.service.worker;

import javax.annotation.Nullable;

/**
 * Captures the stacktrace of exceptions from remote task
 */
public class RemoteTaskException extends Exception {

  private final String remoteExceptionClassName;

  /**
   * @param remoteExceptionClassName the Exception class name that was thrown from the remote task
   * @param message String message for the exception
   * @param cause {@link Throwable} cause for the exception, is nullable.
   */
  public RemoteTaskException(String remoteExceptionClassName, String message, @Nullable Throwable cause) {
    super(message, cause);
    this.remoteExceptionClassName = remoteExceptionClassName;
  }

  public String getRemoteExceptionClassName() {
    return remoteExceptionClassName;
  }

  @Override
  public String toString() {
    String s = getClass().getName() + ": Remote Exception " + getRemoteExceptionClassName();
    String message = getLocalizedMessage();
    return (message != null) ? (s + ": " + message) : s;
  }
}
