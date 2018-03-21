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
import javax.annotation.Nullable;

/**
 * Represents input to the field level operation.
 */
public class Input {
  private final String name;
  private final EndPoint endPoint;

  private Input(String name, @Nullable EndPoint endPoint) {
    this.name = name;
    this.endPoint = endPoint;
  }

  /**
   * Return the Input name. The name could be complete path to the {@link Schema.Field}
   * if it is created from it.
   * @return the name of the Input
   */
  public String getName() {
    return name;
  }

  /**
   * Get the source information if the field is directly read from the dataset, {@code null} otherwise.
   * @return the {@link EndPoint} representing source
   */
  @Nullable
  public EndPoint getEndPoint() {
    return endPoint;
  }

  /**
   * Create an Input from the specified field.
   * @param field the field representing the input
   * @return the Input
   */
  public static Input ofField(Schema.Field field) {
    return ofField(field, null);
  }

  /**
   * Create an Input from the specified field belonging to the given source.
   * @param field the field representing the input
   * @param source the {@link EndPoint} is optional. If it is provided,
   *               this Input from the field will be associated with
   *               the provided source in the lineage.
   * @return the Input
   */
  public static Input ofField(Schema.Field field, @Nullable EndPoint source) {
    // ToDo: Currently we use the name of the field. However we want to generate
    // the complete field path from the field once we add capability to refer to the parent.
    return new Input(field.getName(), source);
  }

  /**
   * Create an Input with the specified name.
   * @param name representing the input
   * @return the Input
   */
  public static Input of(String name) {
    return of(name, null);
  }

  /**
   * Create an Input with specified name and source.
   * @param name the input name
   * @param source the {@link EndPoint} is optional. If it is provided, this Input will
   *               be associated with the provided source in the lineage.
   * @return the Input
   */
  public static Input of(String name, @Nullable EndPoint source) {
    return new Input(name, source);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Input that = (Input) o;

    return Objects.equals(name, that.name) && Objects.equals(endPoint, that.endPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, endPoint);
  }
}
