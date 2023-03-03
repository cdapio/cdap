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

import io.cdap.cdap.proto.BasicThrowable;

/**
 * A exception class for wrapping an {@link Exception} coming from remote task execution.
 * The main purpose of this class is to retain the local stacktrace while using the remote exception message as its
 * own message.
 */
public class RemoteExecutionException extends Exception {

  private final RemoteTaskException cause;

  public RemoteExecutionException(RemoteTaskException cause) {
    super(cause.getMessage(), cause);
    this.cause = cause;
  }

  public RemoteTaskException getCause() {
    return cause;
  }

  /**
   * Converts a {@link BasicThrowable} to a RemoteExecutionException.
   *
   * @return An exception which retains the local stacktrace.
   */
  public static RemoteExecutionException fromBasicThrowable(BasicThrowable basicThrowable) {
    RemoteTaskException remoteTaskException = recursiveWrapBasicThrowable(basicThrowable);
    // Wrap the remote exception as the cause so that we retain the local stacktrace of the exception.
    return new RemoteExecutionException(remoteTaskException);
  }

  private static RemoteTaskException recursiveWrapBasicThrowable(BasicThrowable basicThrowable) {
    BasicThrowable cause = basicThrowable.getCause();
    RemoteTaskException causeException = cause == null ? null : recursiveWrapBasicThrowable(cause);
    RemoteTaskException remoteTaskException = new RemoteTaskException(basicThrowable.getClassName(),
        basicThrowable.getMessage(), causeException);
    remoteTaskException.setStackTrace(basicThrowable.getStackTraces());

    return remoteTaskException;
  }
}
