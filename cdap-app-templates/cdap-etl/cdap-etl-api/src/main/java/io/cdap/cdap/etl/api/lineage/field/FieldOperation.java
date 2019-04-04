/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.lineage.field;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Objects;

/**
 * Abstract base class to represent a field lineage operation. Each operation has a
 * name and description. The name of operation must be unique within all operations
 * recorded by the same pipeline stage. Operation typically has input and output fields.
 */
@Beta
public abstract class FieldOperation {
  private final String name;
  private final OperationType type;
  private final String description;

  protected FieldOperation(String name, OperationType type, String description) {
    this.name = name;
    this.type = type;
    this.description = description;
  }

  /**
   * @return the name of the operation for example, "read" or "concatenate".
   */
  public String getName() {
    return name;
  }

  /**
   * @return the type of the operation
   */
  public OperationType getType() {
    return type;
  }

  /**
   * @return the description associated with the operation
   */
  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldOperation operation = (FieldOperation) o;
    return Objects.equals(name, operation.name) &&
      type == operation.type &&
      Objects.equals(description, operation.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description);
  }
}
