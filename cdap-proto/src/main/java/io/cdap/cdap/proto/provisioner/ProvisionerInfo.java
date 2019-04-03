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

package io.cdap.cdap.proto.provisioner;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Information about a provisioner name and properties.
 */
public class ProvisionerInfo {
  private final String name;
  private final Set<ProvisionerPropertyValue> properties;

  public ProvisionerInfo(String name, Collection<ProvisionerPropertyValue> properties) {
    this.name = name;
    this.properties = Collections.unmodifiableSet(
      properties.stream().filter(Objects::nonNull).collect(Collectors.toSet()));
  }

  public String getName() {
    return name;
  }

  public Set<ProvisionerPropertyValue> getProperties() {
    return properties;
  }

  /**
   * Validate this is a valid object. Should be called when this is created through deserialization of user input.
   *
   * @throws IllegalArgumentException if the object is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Provisioner name must be specified.");
    }
    properties.forEach(ProvisionerPropertyValue::validate);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProvisionerInfo that = (ProvisionerInfo) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties);
  }

  @Override
  public String toString() {
    return "ProvisionerInfo{" +
      "name='" + name + '\'' +
      ", properties=" + properties +
      '}';
  }
}
