/*
 * Copyright © 2015 Cask Data, Inc.
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
import com.google.common.base.Objects;

import java.util.Map;

/**
 * Summary of a {@link DatasetSpecification}. This is returned by the dataset API when getting all dataset instances
 * in a namespace, as we want to hide some of the more detailed information. If more details are required,
 * they are available by getting a specific dataset instance, which will return a {@link DatasetMeta} object.
 */
public class DatasetSpecificationSummary {
  private final String name;
  private final String type;
  private final Map<String, String> properties;

  public DatasetSpecificationSummary(String name, String type, Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.properties = properties;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatasetSpecificationSummary)) {
      return false;
    }

    DatasetSpecificationSummary that = (DatasetSpecificationSummary) o;

    return Objects.equal(name, that.name) &&
      Objects.equal(type, that.type) &&
      Objects.equal(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, type, properties);
  }
}
