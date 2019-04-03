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

import co.cask.cdap.proto.codec.BasicThrowableCodec;

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Maintains basic information about a {@link Throwable}, for easy serialization/deserialization.
 */
public final class BasicThrowable {
  private final String className;
  private final String message;
  private final StackTraceElement[] stackTraces;
  private final BasicThrowable cause;

  /**
   * This constructor is used by {@link BasicThrowableCodec} during de-serialization.
   *
   * @param className the name of the Throwable
   * @param message the message inside the Throwable
   * @param stackTraces stack traces associated with the Throwable
   * @param cause cause associated with the Throwable
   */
  public BasicThrowable(String className,
                        @Nullable String message,
                        StackTraceElement[] stackTraces,
                        @Nullable BasicThrowable cause) {
    this.className = className;
    this.message = message;
    this.stackTraces = stackTraces;
    this.cause = cause;
  }

  /**
   * This constructor is used to create serializable instance from {@link Throwable}.
   *
   * @param throwable the instance of Throwable to be serialize
   */
  public BasicThrowable(Throwable throwable) {
    this.className = throwable.getClass().getName();
    this.message = throwable.getMessage();

    StackTraceElement[] stackTraceElements = throwable.getStackTrace();
    this.stackTraces = new StackTraceElement[stackTraceElements.length];
    System.arraycopy(stackTraceElements, 0, stackTraces, 0, stackTraceElements.length);

    this.cause = (throwable.getCause() == null) ? null : new BasicThrowable(throwable.getCause());
  }

  /**
   * Return the class name for the Throwable.
   */
  public String getClassName() {
    return className;
  }

  /**
   * Return the detail message associated with the Throwable.
   */
  @Nullable
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
  public BasicThrowable getCause() {
    return cause;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BasicThrowable that = (BasicThrowable) o;

    return className.equals(that.className)
      && Objects.equals(message, that.message)
      && Arrays.equals(stackTraces, that.stackTraces)
      && Objects.equals(cause, that.cause);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, message, Arrays.hashCode(stackTraces), cause);
  }
}
