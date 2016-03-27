/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.proto;

import co.cask.cdap.api.dataset.DatasetSpecification;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Summary of a {@link DatasetSpecification}. This is returned by the dataset API when getting all dataset instances
 * in a namespace, as we want to hide some of the more detailed information. If more details are required,
 * they are available by getting a specific dataset instance, which will return a {@link DatasetMeta} object.
 */
public class DatasetSpecificationSummary {
  private final String name;
  private final String type;
  private final String description;
  private final Map<String, String> properties;

  public DatasetSpecificationSummary(String name, String type, @Nullable String description,
                                     Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.properties = properties;
  }

  public DatasetSpecificationSummary(String name, String type, Map<String, String> properties) {
    this(name, type, null, properties);
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetSpecificationSummary that = (DatasetSpecificationSummary) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(description, that.description) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description, properties);
  }

  @Override
  public String toString() {
    return "DatasetSpecificationSummary{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", description='" + description + '\'' +
      ", properties=" + properties +
      '}';
  }
}
