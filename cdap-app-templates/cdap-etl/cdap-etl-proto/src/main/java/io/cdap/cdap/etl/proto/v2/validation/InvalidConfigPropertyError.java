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

import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;

import java.util.Objects;

/**
 * An error that occurred due to an invalid configuration setting for a specific field in a specific pipeline stage.
 */
public class InvalidConfigPropertyError extends StageValidationError {
  private final String property;

  public InvalidConfigPropertyError(String stage, InvalidConfigPropertyException cause) {
    this(cause.getMessage(), stage, cause.getProperty());
  }

  public InvalidConfigPropertyError(String message, String stage, String property) {
    super(Type.INVALID_FIELD, message, stage);
    this.property = property;
  }

  public String getProperty() {
    return property;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    InvalidConfigPropertyError that = (InvalidConfigPropertyError) o;
    return Objects.equals(property, that.property);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), property);
  }
}
