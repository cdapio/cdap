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
 * Represents output in the field level operation.
 */
public class Output {
  private final String name;
  private final EndPoint target;

  private Output(String name, @Nullable EndPoint target) {
    this.name = name;
    this.target = target;
  }

  /**
   * Return the Output name. The name could be complete path to the {@link Schema.Field}
   * if it is created from it.
   * @return the name of the Output
   */
  public String getName() {
    return name;
  }

  /**
   * Get the target information if the field is directly written to the dataset, {@code null} otherwise.
   * @return the {@link EndPoint} representing target
   */
  @Nullable
  public EndPoint getEndPoint() {
    return target;
  }

  /**
   * Create an Output from the specified field.
   *
   * @param field the field representing the output
   * @return the Output
   */
  public static Output ofField(Schema.Field field) {
    return ofField(field, null);
  }

  /**
   * Create an Output from the specified field belonging to the given target.
   * @param field the field representing the output
   * @param target the {@link EndPoint} is optional. If it is provided,
   *               this Output from the field will be associated with
   *               the provided target in the lineage.
   * @return the Output
   */
  public static Output ofField(Schema.Field field, @Nullable EndPoint target) {
    // ToDo: Currently we use the name of the field. However we want to generate
    // the complete field path from the field once we add capability to refer to the parent.
    return new Output(field.getName(), target);
  }

  /**
   * Create an Output with the specified name.
   * @param name representing the output
   * @return the Output
   */
  public static Output of(String name) {
    return of(name, null);
  }

  /**
   * Create an Output with specified name and target.
   * @param name the output name
   * @param target the {@link EndPoint} is optional. If it is provided, this Output will
   *               be associated with the provided target in the lineage.
   * @return the Output
   */
  public static Output of(String name, @Nullable EndPoint target) {
    return new Output(name, target);
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

    return Objects.equals(name, that.name) && Objects.equals(target, that.target);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, target);
  }
}
