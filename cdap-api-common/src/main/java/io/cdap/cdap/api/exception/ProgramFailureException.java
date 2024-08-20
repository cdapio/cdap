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
 * Exception class representing a program failure with detailed error information.
 * <p>
 * This exception is used to indicate failures within the system and provides details about
 * the error category, message, reason, and type. It uses the builder pattern to allow
 * for flexible construction of the exception with optional fields.
 * </p>
 * <p>
 * The following details can be provided:
 * </p>
 * <ul>
 *   <li><b>errorCategory:</b> A high-level classification of the error.</li>
 *   <li><b>errorMessage:</b> A detailed description of the error message.</li>
 *   <li><b>errorReason:</b> A reason for why the error occurred.</li>
 *   <li><b>errorType:</b> The type of error, represented by the {@ErrorType} enum,
 *   such as SYSTEM, USER, or UNKNOWN.</li>
 * </ul>
 **/
public class ProgramFailureException extends RuntimeException {
  private final String errorCategory;
  private final String errorMessage;
  private final String errorReason;
  private final ErrorType errorType;

  // Private constructor to prevent direct instantiation
  private ProgramFailureException(String errorCategory, String errorMessage, String errorReason,
      ErrorType errorType) {
    this.errorCategory = errorCategory;
    this.errorMessage = errorMessage;
    this.errorReason = errorReason;
    this.errorType = errorType;
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
  public String getErrorCategory() {
    return errorCategory == null ? "Others" : errorCategory;
  }

  /**
   * Returns the detailed message associated with the error.
   *
   * @return a {@String} representing the error message.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Returns the reason for the error.
   *
   * <p>The reason usually explains why the error occurred, such as a specific validation failure
   * or an unexpected condition.
   *
   * @return a {@String} representing the error reason.
   */
  public String getErrorReason() {
    return errorReason;
  }

  /**
   * Returns the type of the error.
   *
   * <p>This method provides information on whether the error is classified as a
   * system-level error, a user-caused error, or an unknown type of error.
   *
   * @return an {@ErrorType} enum value representing the type of the error.
   */
  public ErrorType getErrorType() {
    return errorType == null ? ErrorType.UNKNOWN : errorType;
  }

  /**
   * Builder class for ProgramFailureException.
   */
  public static class Builder {
    private String errorCategory;
    private String errorMessage;
    private String errorReason;
    private ErrorType errorType;

    /**
     * Sets the error category for the ProgramFailureException.
     *
     * @param errorCategory The category of the error.
     * @return The current Builder instance.
     */
    public Builder withErrorCategory(String errorCategory) {
      this.errorCategory = errorCategory;
      return this;
    }

    /**
     * Sets the error message for the ProgramFailureException.
     *
     * @param errorMessage The detailed error message.
     * @return The current Builder instance.
     */
    public Builder withErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    /**
     * Sets the error reason for the ProgramFailureException.
     *
     * @param errorReason The reason for the error.
     * @return The current Builder instance.
     */
    public Builder withErrorReason(String errorReason) {
      this.errorReason = errorReason;
      return this;
    }

    /**
     * Sets the error type for the ProgramFailureException.
     *
     * @param errorType The type of error (SYSTEM, USER, UNKNOWN).
     * @return The current Builder instance.
     */
    public Builder withErrorType(ErrorType errorType) {
      this.errorType = errorType;
      return this;
    }

    /**
     * Builds and returns a new instance of ProgramFailureException.
     *
     * @return A new ProgramFailureException instance.
     */
    public ProgramFailureException build() {
      return new ProgramFailureException(errorCategory, errorMessage, errorReason, errorType);
    }
  }
}
