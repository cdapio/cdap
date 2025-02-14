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

package io.cdap.cdap.api.macro;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import javax.annotation.Nullable;

/**
 * Indicates that there is an invalid macro.
 */
public class InvalidMacroException extends RuntimeException implements FailureDetailsProvider {

  private final ErrorCategory errorCategory;

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(String message) {
    this(message, null, null);
  }

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(String message, Throwable cause) {
    this(message, cause, null);
  }

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(Throwable cause) {
    this(cause.getMessage(), cause, null);
  }

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(Throwable cause, ErrorCategory errorCategory) {
    this(cause.getMessage(), cause, errorCategory);
  }

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(String message, ErrorCategory errorCategory) {
    this(message, null, errorCategory);
  }

  /**
   * Constructor for {@link InvalidMacroException}.
   */
  public InvalidMacroException(String message, @Nullable Throwable cause,
      @Nullable ErrorCategory errorCategory) {
    super(message, cause);
    this.errorCategory = errorCategory;
  }


  @Override
  public ErrorCategory getErrorCategory() {
    return errorCategory == null ?  new ErrorCategory(ErrorCategoryEnum.MACROS) : errorCategory;
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.USER;
  }

  @Override
  public String getErrorReason() {
    return getMessage();
  }
}
