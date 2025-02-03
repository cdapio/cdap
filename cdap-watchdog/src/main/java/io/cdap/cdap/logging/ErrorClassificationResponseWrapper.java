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

package io.cdap.cdap.logging;

import io.cdap.cdap.proto.ErrorClassificationResponse;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Wrapper class for {@link ErrorClassificationResponse}.
 */
public final class ErrorClassificationResponseWrapper {
  private final ErrorClassificationResponse errorClassificationResponse;
  private final String parentErrorCategory;
  private final String throwableClassName;
  private final Integer rulePriority;
  private final String ruleId;

  /**
   * Constructor for {@link ErrorClassificationResponseWrapper}.
   */
  public ErrorClassificationResponseWrapper(ErrorClassificationResponse response,
      String parentErrorCategory, String throwableClassName, @Nullable Integer rulePriority,
      @Nullable String ruleId) {
    this.errorClassificationResponse = response;
    this.parentErrorCategory = parentErrorCategory;
    this.throwableClassName = throwableClassName;
    this.ruleId = ruleId;
    this.rulePriority = rulePriority;
  }

  /**
   * Gets the error classification response for ErrorClassificationResponse.
   */
  public ErrorClassificationResponse getErrorClassificationResponse() {
    return errorClassificationResponse;
  }

  /**
   * Gets the parent error category for ErrorClassificationResponse.
   */
  public String getParentErrorCategory() {
    return parentErrorCategory;
  }

  /**
   * Gets the throwable class name for ErrorClassificationResponse.
   */
  public String getThrowableClassName() {
    return throwableClassName;
  }

  /**
   * Gets the rule priority for ErrorClassificationResponse.
   */
  public Integer getRulePriority() {
    return rulePriority == null ? Integer.MAX_VALUE : rulePriority;
  }

  /**
   * Gets the rule id for ErrorClassificationResponse.
   */
  public String getRuleId() {
    return ruleId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ErrorClassificationResponseWrapper)) {
      return false;
    }
    ErrorClassificationResponseWrapper that = (ErrorClassificationResponseWrapper) o;

    return Objects.equals(this.errorClassificationResponse, that.errorClassificationResponse)
        && Objects.equals(this.throwableClassName, that.throwableClassName)
        && Objects.equals(this.rulePriority, that.rulePriority)
        && Objects.equals(this.ruleId, that.ruleId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorClassificationResponse, throwableClassName, rulePriority, ruleId);
  }
}
