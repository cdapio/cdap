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

package io.cdap.cdap.etl.api.join.error;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * An error with one of the output schema fields.
 */
public class OutputSchemaError extends JoinError {
  private final String field;
  private final String expectedType;

  public OutputSchemaError(String field, @Nullable String expectedType, String message) {
    this(field, expectedType, message, null);
  }

  public OutputSchemaError(String field, @Nullable String expectedType,
                           String message, @Nullable String correctiveAction) {
    super(Type.OUTPUT_SCHEMA, message, correctiveAction);
    this.field = field;
    this.expectedType = expectedType;
  }

  public String getField() {
    return field;
  }

  @Nullable
  public String getExpectedType() {
    return expectedType;
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
    OutputSchemaError that = (OutputSchemaError) o;
    return Objects.equals(field, that.field) &&
      Objects.equals(expectedType, that.expectedType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), field, expectedType);
  }
}
