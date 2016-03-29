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
package co.cask.cdap.proto;

import javax.annotation.Nullable;

/**
 * Carries {@link Throwable} information in {@link WorkflowNodeStateDetail}.
 */
public final class DefaultThrowable {
  private final String className;
  private final String message;
  private final StackTraceElement[] stackTraces;
  private final DefaultThrowable cause;

  public DefaultThrowable(String className, String message, StackTraceElement[] stackTraces, DefaultThrowable cause) {
    this.className = className;
    this.message = message;
    this.stackTraces = stackTraces;
    this.cause = cause;
  }

  /**
   * Creates serializable instance from {@link Throwable}.
   */
  public DefaultThrowable(Throwable throwable) {
    this.className = throwable.getClass().getName();
    this.message = throwable.getMessage();

    StackTraceElement[] stackTraceElements = throwable.getStackTrace();
    this.stackTraces = new StackTraceElement[stackTraceElements.length];
    System.arraycopy(stackTraceElements, 0, stackTraces, 0, stackTraceElements.length);

    this.cause = (throwable.getCause() == null) ? null : new DefaultThrowable(throwable.getCause());
  }

  /**
   * Return the class name for the Throwablne.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Return the detail message associated with the Throwable.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Return the stack traces.
   */
  public StackTraceElement[] getStackTraces() {
    return stackTraces;
  }

  /**
   * Return the cause of Throwable if it is available, otherwise {@code null}.
   */
  @Nullable
  public DefaultThrowable getCause() {
    return cause;
  }
}
