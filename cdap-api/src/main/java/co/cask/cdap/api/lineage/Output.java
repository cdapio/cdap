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
  private final Destination destination;

  private Output(String name, @Nullable Destination destination) {
    this.name = name;
    this.destination = destination;
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
   * Get the Destination information if the field is directly written to the dataset, {@code null} otherwise.
   * @return the Destination
   */
  @Nullable
  public Destination getDestination() {
    return destination;
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
   * Create an Output from the specified field belonging to the given destination.
   * @param field the field representing the output
   * @param destination the {@link Destination} is optional. If it is provided,
   *                    this Output from the field will be associated with
   *                    the provided destination in the lineage.
   * @return the Output
   */
  public static Output ofField(Schema.Field field, @Nullable Destination destination) {
    // ToDo: Currently we use the name of the field. However we want to generate
    // the complete field path from the field once we add capability to refer to the parent.
    return new Output(field.getName(), destination);
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
   * Create an Output with specified name and destination.
   * @param name the output name
   * @param destination the {@link Destination} is optional. If it is provided, this Output will
   *                    be associated with the provided destination in the lineage.
   * @return the Output
   */
  public static Output of(String name, @Nullable Destination destination) {
    return new Output(name, destination);
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

    return Objects.equals(name, that.name) && Objects.equals(destination, that.destination);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, destination);
  }
}
