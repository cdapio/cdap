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
package co.cask.cdap.api.lineage;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Objects;

/**
 * Represents output in the field level operation.
 */
public class Output {
  private final Schema.Field field;

  private Output(Schema.Field field) {
    this.field = field;
  }

  /**
   * Create an Output from the specified field.
   *
   * @param field the field representing the output
   * @return the Output
   */
  public static Output ofField(Schema.Field field) {
    return new Output(field);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Output that = (Output) o;

    return Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field);
  }
}
