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

import com.google.common.base.Strings;
import io.cdap.cdap.api.exception.ErrorType;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Represents the rule for classifying error logs.
 */
public final class ErrorClassificationRule implements Comparable<ErrorClassificationRule> {
  public static final Pattern DEFAULT_PATTERN = Pattern.compile(".*");
  private final String id;
  private final String description;
  private final int priority;
  private final String exceptionClassRegex;
  private final String codeMethodRegex;
  private final ErrorType errorType;
  private final boolean dependency;
  private Pattern exceptionClassRegexPattern;
  private Pattern codeMethodRegexPattern;


  private ErrorClassificationRule(String id, String description, int priority,
      @Nullable String exceptionClassRegex, @Nullable String codeMethodRegex,
      ErrorType errorType, boolean dependency) {
    this.id = id;
    this.description = description;
    this.priority = priority;
    this.exceptionClassRegex = exceptionClassRegex;
    this.codeMethodRegex = codeMethodRegex;
    this.errorType = errorType;
    this.dependency = dependency;
  }

  /**
   * Returns the id for ErrorClassificationRule.
   */
  public String getId() {
    return id;
  }

  /**
   * Returns the description for ErrorClassificationRule.
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns the priority for ErrorClassificationRule.
   */
  public int getPriority() {
    return priority;
  }

  /**
   * Returns the exception class regex pattern for ErrorClassificationRule.
   */
  public Pattern getExceptionClassRegex() {
    if (this.exceptionClassRegexPattern == null) {
      this.exceptionClassRegexPattern =
          Strings.isNullOrEmpty(this.exceptionClassRegex) ? DEFAULT_PATTERN
              : Pattern.compile(this.exceptionClassRegex);
    }
    return this.exceptionClassRegexPattern;
  }

  /**
   * Returns the code method regex pattern for ErrorClassificationRule.
   */
  public Pattern getCodeMethodRegex() {
    if (this.codeMethodRegexPattern == null) {
      this.codeMethodRegexPattern = Strings.isNullOrEmpty(this.codeMethodRegex) ? DEFAULT_PATTERN
          : Pattern.compile(this.codeMethodRegex);
    }
    return this.codeMethodRegexPattern;
  }

  /**
   * Returns the {@link ErrorType} for ErrorClassificationRule.
   */
  public ErrorType getErrorType() {
    return errorType == null ? ErrorType.UNKNOWN : errorType;
  }

  /**
   * Returns the dependency flag for ErrorClassificationRule.
   */
  public boolean isDependency() {
    return dependency;
  }

  @Override
  public int compareTo(ErrorClassificationRule that) {
    return Integer.compare(this.priority, that.priority);
  }

  @Override
  public String toString() {
    return String.format("Rule Id: '%s', Description: '%s', Exception Class Name: '%s',"
        + "Code Method Regex: '%s', Error Type: '%s', Dependency: '%s'", id, description,
        getExceptionClassRegex().pattern(), getCodeMethodRegex().pattern(), errorType, dependency);
  }

  /**
   * Builder class for {@link ErrorClassificationRule}.
   */
  public static class Builder {
    private String id;
    private String description;
    private int priority;
    private String exceptionClassRegex;
    private String codeMethodRegex;
    private ErrorType errorType;
    private boolean dependency;

    /**
     * Sets the id for ErrorClassificationRule.
     */
    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    /**
     * Sets the description for ErrorClassificationRule.
     */
    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the priority for ErrorClassificationRule.
     */
    public Builder setPriority(int priority) {
      this.priority = priority;
      return this;
    }

    /**
     * Sets the exception class name regex for ErrorClassificationRule.
     */
    public Builder setExceptionClassName(String exceptionClassRegex) {
      this.exceptionClassRegex = exceptionClassRegex;
      return this;
    }

    /**
     * Sets the code method regex for ErrorClassificationRule.
     */
    public Builder setCodeMethodRegex(String codeMethodRegex) {
      this.codeMethodRegex = codeMethodRegex;
      return this;
    }

    /**
     * Sets the {@link ErrorType} for ErrorClassificationRule.
     */
    public Builder setErrorType(ErrorType errorType) {
      this.errorType = errorType;
      return this;
    }

    /**
     * Sets the dependency flag for ErrorClassificationRule.
     */
    public Builder setDependency(boolean dependency) {
      this.dependency = dependency;
      return this;
    }

    public ErrorClassificationRule build() {
      return new ErrorClassificationRule(id, description, priority, exceptionClassRegex,
          codeMethodRegex, errorType, dependency);
    }
  }
}
