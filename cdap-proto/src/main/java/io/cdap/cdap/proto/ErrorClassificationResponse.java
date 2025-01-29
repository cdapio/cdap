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

package io.cdap.cdap.proto;

import java.util.Objects;

/**
 * Represents the response for classifying error logs.
 */
public final class ErrorClassificationResponse {
  private final String stageName;
  private final String errorCategory;
  private final String errorReason;
  private final String errorMessage;
  private final String errorType;
  private final String dependency;
  private final String errorCodeType;
  private final String errorCode;
  private final String supportedDocumentationUrl;
  private transient final String throwableClassName;

  private ErrorClassificationResponse(String stageName, String errorCategory, String errorReason,
      String errorMessage, String errorType, String dependency, String errorCodeType,
      String errorCode, String supportedDocumentationUrl, String throwableClassName) {
    this.stageName = stageName;
    this.errorCategory = errorCategory;
    this.errorReason = errorReason;
    this.errorMessage = errorMessage;
    this.errorType = errorType;
    this.dependency = dependency;
    this.errorCodeType = errorCodeType;
    this.errorCode = errorCode;
    this.supportedDocumentationUrl = supportedDocumentationUrl;
    this.throwableClassName = throwableClassName;
  }

  /**
   * Gets the stage name for ErrorClassificationResponse.
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * Gets the error category for ErrorClassificationResponse.
   */
  public String getErrorCategory() {
    return errorCategory;
  }

  /**
   * Gets the error reason for ErrorClassificationResponse.
   */
  public String getErrorReason() {
    return errorReason;
  }

  /**
   * Gets the error message for ErrorClassificationResponse.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Gets the error type for ErrorClassificationResponse.
   */
  public String getErrorType() {
    return errorType;
  }

  /**
   * Gets the dependency flag for ErrorClassificationResponse.
   */
  public String getDependency() {
    return dependency;
  }

  /**
   * Gets the error code type for ErrorClassificationResponse.
   */
  public String getErrorCodeType() {
    return errorCodeType;
  }

  /**
   * Gets the error code for ErrorClassificationResponse.
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * Gets the supported documentation URL for ErrorClassificationResponse.
   */
  public String getSupportedDocumentationUrl() {
    return supportedDocumentationUrl;
  }

  /**
   * Gets the throwable class name for ErrorClassificationResponse.
   */
  public String getThrowableClassName() {
    return throwableClassName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ErrorClassificationResponse)) {
      return false;
    }
    ErrorClassificationResponse that = (ErrorClassificationResponse) o;
    return Objects.equals(this.stageName, that.stageName)
        && Objects.equals(this.errorCategory, that.errorCategory)
        && Objects.equals(this.errorReason, that.errorReason)
        && Objects.equals(this.errorMessage, that.errorMessage)
        && Objects.equals(this.errorType, that.errorType)
        && Objects.equals(this.dependency, that.dependency)
        && Objects.equals(this.errorCodeType, that.errorCodeType)
        && Objects.equals(this.errorCode, that.errorCode)
        && Objects.equals(this.supportedDocumentationUrl, that.supportedDocumentationUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageName, errorCategory, errorReason, errorMessage, errorType,
        dependency, errorCodeType, errorCode, supportedDocumentationUrl);
  }

  /**
   * Builder for {@link ErrorClassificationResponse}.
   */
  public static class Builder {
    private String stageName;
    private String errorCategory;
    private String errorReason;
    private String errorMessage;
    private String errorType;
    private String dependency;
    private String errorCodeType;
    private String errorCode;
    private String supportedDocumentationUrl;
    private String throwableClassName;

    /**
     * Sets the stage name for ErrorClassificationResponse.
     */
    public Builder setStageName(String stageName) {
      this.stageName = stageName;
      return this;
    }

    /**
     * Sets the error category for ErrorClassificationResponse.
     */
    public Builder setErrorCategory(String errorCategory) {
      this.errorCategory = errorCategory;
      return this;
    }

    /**
     * Sets the error reason for ErrorClassificationResponse.
     */
    public Builder setErrorReason(String errorReason) {
      this.errorReason = errorReason;
      return this;
    }

    /**
     * Sets the error message for ErrorClassificationResponse.
     */
    public Builder setErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    /**
     * Sets the error type for ErrorClassificationResponse.
     */
    public Builder setErrorType(String errorType) {
      this.errorType = errorType;
      return this;
    }

    /**
     * Sets the dependency flag for ErrorClassificationResponse.
     */
    public Builder setDependency(String dependency) {
      this.dependency = dependency;
      return this;
    }

    /**
     * Sets the error code type for ErrorClassificationResponse.
     */
    public Builder setErrorCodeType(String errorCodeType) {
      this.errorCodeType = errorCodeType;
      return this;
    }

    /**
     * Sets the error code for ErrorClassificationResponse.
     */
    public Builder setErrorCode(String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    /**
     * Sets the supported documentation URL for ErrorClassificationResponse.
     */
    public Builder setSupportedDocumentationUrl(String supportedDocumentationUrl) {
      this.supportedDocumentationUrl = supportedDocumentationUrl;
      return this;
    }

    /**
     * Sets the throwable class name for ErrorClassificationResponse.
     */
    public Builder setThrowableClassName(String throwableClassName) {
      this.throwableClassName = throwableClassName;
      return this;
    }

    /**
     * Builds and returns a new instance of ErrorClassificationResponse.
     */
    public ErrorClassificationResponse build() {
      return new ErrorClassificationResponse(stageName, errorCategory, errorReason, errorMessage,
          errorType, dependency, errorCodeType, errorCode, supportedDocumentationUrl,
          throwableClassName);
    }
  }
}
