/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.api.exception;

/**
 * Exception class that wraps another exception and includes the name of the stage
 * where the failure occurred.
 * <p>
 * This class extends {@code RuntimeException} and is used to provide additional context when
 * an error occurs during a specific stage of execution.
 * The original cause of the error is preserved, and the stage name where the failure happened is
 * included for better error tracking.
 * </p>
 */
public class WrappedStageException extends RuntimeException {

  private final String stageName;

  /**
   * Constructs a new {@code WrappedStageException} with the specified cause and stage name.
   *
   * @param cause the original exception that caused the failure.
   * @param stageName the name of the stage where the failure occurred.
   */
  public WrappedStageException(Throwable cause, String stageName) {
    super(cause);
    this.stageName = stageName;
  }

  /**
   * Returns the name of the stage where the exception occurred.
   *
   * @return the stage name as a {@String}.
   */
  public String getStageName() {
    return stageName;
  }
}
