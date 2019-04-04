/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.v2.validation;

import java.util.Objects;

/**
 * Represents some sort of error that occurred during validation.
 */
public class ValidationError {
  protected final Type type;
  protected final String message;

  /**
   * Types of validation errors
   */
  public enum Type {
    ERROR,
    STAGE_ERROR,
    INVALID_FIELD,
    PLUGIN_NOT_FOUND
  }

  public ValidationError(String message) {
    this(Type.ERROR, message);
  }

  public ValidationError(Type type, String message) {
    this.type = type;
    this.message = message;
  }

  public Type getType() {
    return type;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValidationError that = (ValidationError) o;
    return type == that.type &&
      Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, message);
  }
}
