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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The schema for a pipeline stage.
 */
public class StageSchema {
  private final String stage;
  private final Schema schema;

  public StageSchema(String stage, @Nullable Schema schema) {
    this.stage = stage;
    this.schema = schema;
  }

  public String getStage() {
    return stage;
  }

  @Nullable
  public Schema getSchema() {
    return schema;
  }

  /**
   * Validate that the stage schema is valid. This should be called whenever the instance was created by deserializing
   * user input.
   *
   * @throws IllegalArgumentException if the stage schema is invalid
   */
  public void validate() {
    if (stage == null || stage.isEmpty()) {
      throw new IllegalArgumentException("A stage name must be provided along with the schema.");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StageSchema that = (StageSchema) o;
    return Objects.equals(stage, that.stage) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stage, schema);
  }
}
