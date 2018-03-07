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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the information about the destination dataset.
 */
public class Destination {
  private final String name;
  private final Map<String, String> properties;
  private String namespace;

  private Destination(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = Collections.unmodifiableMap(properties);
  }

  /**
   * Sets the namespace of the destination.
   * @param namespace the namespace of the destination
   * @return the destination being operated on
   */
  public Destination fromNamespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  /**
   * Return the destination as defined by the provided dataset name.
   * @param datasetName the name of the dataset
   * @return the destination
   */
  public static Destination ofDataset(String datasetName) {
    return ofDataset(datasetName, Collections.emptyMap());
  }

  /**
   * Return the destination as defined by the provided dataset.
   * @param datasetName the name of the dataset
   * @param properties the properties to be associated with the destination for lineage purpose
   */
  public static Destination ofDataset(String datasetName, Map<String, String> properties) {
    return new Destination(datasetName, properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Destination that = (Destination) o;

    return Objects.equals(name, that.name)
      && Objects.equals(namespace, that.namespace)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, namespace, properties);
  }
}
