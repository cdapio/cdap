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

package co.cask.cdap.api;

import java.util.Objects;

/**
 * Information about a namespace.
 */
public class NamespaceSummary {
  private final String name;
  private final String description;
  private final long generation;

  public NamespaceSummary(String name, String description, long generation) {
    this.name = name;
    this.description = description;
    this.generation = generation;
  }

  /**
   * @return the namespace name, unique across namespaces
   */
  public String getName() {
    return name;
  }

  /**
   * @return the namespace description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Get the namespace generation. The generation is set when the namespace is created. If the namespace is deleted
   * and then created again, it will have a higher generation.
   *
   * @return the namespace generation
   */
  public long getGeneration() {
    return generation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NamespaceSummary that = (NamespaceSummary) o;
    return generation == that.generation &&
      Objects.equals(name, that.name) &&
      Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, generation);
  }

  @Override
  public String toString() {
    return "NamespaceSummary{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", generation=" + generation +
      '}';
  }
}
