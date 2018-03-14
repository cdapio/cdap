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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the information about the source dataset.
 */
public class Source {
  private final String name;
  private final String namespace;
  private final Map<String, String> properties;

  private Source(String name, @Nullable String namespace, Map<String, String> properties) {
    this.name = name;
    this.namespace = namespace;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  /**
   * @return the name of the source dataset
   */
  public String getName() {
    return name;
  }

  /**
   * @return the properties associated with the source for the lineage purpose
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @return the namespace name if the source exist in different namespace,
   * otherwise {@code null} is returned
   */
  @Nullable
  public String getNamespace() {
    return namespace;
  }

  /**
   * Return the source as defined by the provided dataset name.
   * @param datasetName the name of the dataset
   * @return the Source
   */
  public static Source ofDataset(String datasetName) {
    return ofDataset(datasetName, null, Collections.emptyMap());
  }

  /**
   * Return the source as defined by the provided dataset name.
   * @param datasetName the name of the dataset
   * @param namespace the name of the namespace
   * @return the Source
   */
  public static Source ofDataset(String datasetName, String namespace) {
    return ofDataset(datasetName, namespace, Collections.emptyMap());
  }

  /**
   * Return the source as defined by the provided dataset.
   * @param datasetName the name of the dataset
   * @param properties the properties to be associated with the source for lineage purpose
   */
  public static Source ofDataset(String datasetName, Map<String, String> properties) {
    return ofDataset(datasetName, null, properties);
  }

  /**
   * Return the source as defined by the provided dataset name.
   * @param datasetName the name of the dataset
   * @param namespace the name of the namespace
   * @param properties the properties to be associated with the source for lineage purpose
   * @return the Source
   */
  public static Source ofDataset(String datasetName, String namespace, Map<String, String> properties) {
    return new Source(datasetName, namespace, properties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Source that = (Source) o;

    return Objects.equals(name, that.name)
      && Objects.equals(namespace, that.namespace)
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, namespace, properties);
  }
}
