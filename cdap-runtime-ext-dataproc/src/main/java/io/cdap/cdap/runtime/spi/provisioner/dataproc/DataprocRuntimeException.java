/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.gax.rpc.ApiException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import javax.annotation.Nullable;

/**
 * A {@link RuntimeException} that wraps exceptions from Dataproc operation and provide a {@link
 * #toString()} implementation that doesn't include this exception class name and with the root
 * cause error message.
 */
public class DataprocRuntimeException extends RuntimeException implements FailureDetailsProvider {

  private static final String TROUBLESHOOTING_MESSAGE_FORMAT =
    "For troubleshooting Dataproc errors, refer to %s";
  public static final String TROUBLESHOOTING_DOC_URL =
      "https://cloud.google.com/dataproc/docs/support/troubleshoot-cluster-creation";
  public static final ErrorCategory ERROR_CATEGORY_PROVISIONING_CONFIGURATION =
      new ErrorCategory(ErrorCategoryEnum.PROVISIONING, "Configuration");

  private final ErrorCategory errorCategory;
  private final String errorReason;
  private final ErrorType errorType;
  private final boolean dependency;
  private final ErrorCodeType errorCodeType;
  private final String errorCode;
  private final String supportedDocumentationUrl;

  private DataprocRuntimeException(@Nullable String operationId, Throwable cause,
      ErrorCategory errorCategory, String errorReason, String errorMessage, ErrorType errorType,
      boolean dependency, ErrorCodeType errorCodeType, String errorCode,
      String supportedDocumentationUrl) {
    super(createMessage(operationId, errorMessage, dependency), cause);
    this.errorCategory = errorCategory;
    this.errorReason = errorReason;
    this.errorType = errorType;
    this.dependency = dependency;
    this.errorCodeType = errorCodeType;
    this.errorCode = errorCode;
    this.supportedDocumentationUrl = supportedDocumentationUrl;
  }

  private static String createMessage(@Nullable String operationId, String errorMessage,
      boolean dependency) {
    StringBuilder message = new StringBuilder();
    if (operationId != null) {
      message.append("Dataproc operation ")
          .append(operationId)
          .append(" failure: ")
          .append(errorMessage);
    } else if (dependency) {
      message.append("Dataproc operation failure: ")
          .append(errorMessage);
    } else {
      message.append(errorMessage);
    }
    return message.toString();
  }

  @Nullable
  @Override
  public String getErrorReason() {
    if (!Strings.isNullOrEmpty(errorReason)) {
      if (errorReason.endsWith(".") && !Strings.isNullOrEmpty(getSupportedDocumentationUrl())) {
       return String.format("%s %s", errorReason,
           String.format(TROUBLESHOOTING_MESSAGE_FORMAT, getSupportedDocumentationUrl()));
      }
    }
    return errorReason;
  }

  @Nullable
  @Override
  public String getFailureStage() {
    return null;
  }

  @Override
  public ErrorCategory getErrorCategory() {
    if (errorCategory == null) {
      return FailureDetailsProvider.super.getErrorCategory();
    }
    return errorCategory;
  }

  @Override
  public ErrorType getErrorType() {
    if (errorType == null) {
      return FailureDetailsProvider.super.getErrorType();
    }
    return errorType;
  }

  @Override
  public boolean isDependency() {
    return dependency;
  }

  @Nullable
  @Override
  public ErrorCodeType getErrorCodeType() {
    return errorCodeType;
  }

  @Nullable
  @Override
  public String getErrorCode() {
    return errorCode;
  }

  @Nullable
  @Override
  public String getSupportedDocumentationUrl() {
    Throwable cause = getCause();
    if (cause != null & cause instanceof ApiException) {
      if (Strings.isNullOrEmpty(supportedDocumentationUrl)) {
        return TROUBLESHOOTING_DOC_URL;
      }
    }
    return supportedDocumentationUrl;
  }

  /**
   * Builder class for DataprocRuntimeException.
   */
  public static class Builder {
    private ErrorCategory errorCategory;
    private String errorReason;
    private String errorMessage;
    private ErrorType errorType;
    private Throwable cause;
    private ErrorCodeType errorCodeType;
    private String errorCode;
    private boolean dependency;
    private String supportedDocumentationUrl;
    private String operationId;

    /**
     * Sets the error category for the DataprocRuntimeException.
     *
     * @param errorCategory The category of the error.
     * @return The current Builder instance.
     */
    public Builder withErrorCategory(ErrorCategory errorCategory) {
      this.errorCategory = errorCategory;
      return this;
    }

    /**
     * Sets the error reason for the DataprocRuntimeException.
     *
     * @param errorReason The reason for the error.
     * @return The current Builder instance.
     */
    public Builder withErrorReason(String errorReason) {
      this.errorReason = errorReason;
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
     * Sets the error type for the DataprocRuntimeException.
     *
     * @param errorType The type of error (SYSTEM, USER, UNKNOWN).
     * @return The current Builder instance.
     */
    public Builder withErrorType(ErrorType errorType) {
      this.errorType = errorType;
      return this;
    }

    /**
     * Sets the cause for the DataprocRuntimeException.
     *
     * @param cause the cause (which is saved for later retrieval by the getCause() method).
     * @return The current Builder instance.
     */
    public Builder withCause(Throwable cause) {
      this.cause = cause;
      return this;
    }

    /**
     * Sets the dependency flag for the DataprocRuntimeException.
     *
     * @param dependency True if the error is from a dependent service, false otherwise.
     * @return The current Builder instance.
     */
    public Builder withDependency(boolean dependency) {
      this.dependency = dependency;
      return this;
    }

    /**
     * Sets the error code type for the DataprocRuntimeException.
     *
     * @param errorCodeType The type of error code.
     * @return The current Builder instance.
     */
    public Builder withErrorCodeType(ErrorCodeType errorCodeType) {
      this.errorCodeType = errorCodeType;
      return this;
    }

    /**
     * Sets the error code for the DataprocRuntimeException.
     *
     * @param errorCode The error code.
     * @return The current Builder instance.
     */
    public Builder withErrorCode(String errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    /**
     * Sets the supported documentation URL for the DataprocRuntimeException.
     *
     * @param supportedDocumentationUrl The URL to the documentation.
     * @return The current Builder instance.
     */
    public Builder withSupportedDocumentationUrl(String supportedDocumentationUrl) {
      this.supportedDocumentationUrl = supportedDocumentationUrl;
      return this;
    }

    /**
     * Sets the dataproc operationId for the DataprocRuntimeException.
     *
     * @param operationId the dataproc operationId.
     * @return The current Builder instance.
     */
    public Builder withOperationId(String operationId) {
      this.operationId = operationId;
      return this;
    }

    /**
     * Builds and returns a new instance of DataprocRuntimeException.
     *
     * @return A new DataprocRuntimeException instance.
     */
    public DataprocRuntimeException build() {
      return new DataprocRuntimeException(operationId, cause, errorCategory, errorReason,
          errorMessage, errorType, dependency, errorCodeType, errorCode, supportedDocumentationUrl);
    }
  }
}
