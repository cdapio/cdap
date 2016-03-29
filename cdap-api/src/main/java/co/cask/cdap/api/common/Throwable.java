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
package co.cask.cdap.api.common;

/**
 * Carries {@link java.lang.Throwable} information for serialization and deserialization purpose.
 */
public interface Throwable {
  /**
   * Returns the name of the Throwable class.
   */
  String getClassName();

  /**
   * Returns the message contained inside the Throwable.
   *
   * @return A {@link String} message or {@code null} if such message is not available.
   */
  String getMessage();

  /**
   * Returns the stack trace of the Throwable.
   */
  StackTraceElement[] getStackTraces();

  /**
   * Returns the cause of this {@link Throwable}.
   *
   * @return The {@link Throwable} cause or {@code null} if no cause is available.
   */
  Throwable getCause();
}
