/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import javax.annotation.Nullable;

/**
 * Interface for providing failure details.
 */
public interface FailureDetailsProvider {

  /**
   * Returns the reason for the error.
   *
   * <p>The reason usually explains why the error occurred, such as a specific validation failure
   * or an unexpected condition.
   *
   * @return a {@String} representing the error reason.
   */
  @Nullable
  default String getErrorReason() {
    return null;
  }

  /**
   * Returns the stage where the failure occurred.
   */
  @Nullable
  default String getFailureStage() {
    return null;
  }

  /**
   * Returns the category of the error.
   *
   * <p>This typically provides a high-level classification of the error,
   * such as plugin, provisioning, etc.
   * If the category or reason is not known - it will be marked as  ‘Others’.
   *
   * @return a {@String} representing the error category.
   */
  default ErrorCategory getErrorCategory() {
    return new ErrorCategory(ErrorCategoryEnum.OTHERS);
  }

  /**
   * Returns the type of the error.
   *
   * <p>This method provides information on whether the error is classified as a
   * system-level error, a user-caused error, or an unknown type of error.
   *
   * @return an {@ErrorType} enum value representing the type of the error.
   */
  default ErrorType getErrorType() {
    return ErrorType.UNKNOWN;
  }
}
