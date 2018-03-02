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

package co.cask.cdap.runtime.spi.provisioner;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Specification of a Provisioner. Includes information about the provisioner as a whole as well as information about
 * every property supported by the provisioner.
 */
public class ProvisionerSpecification {
  private final String name;
  private final String label;
  private final String description;
  private final Map<String, ProvisionerProperty> properties;

  public ProvisionerSpecification(String name, String label, String description,
                                  Map<String, ProvisionerProperty> properties) {
    this.name = name;
    this.label = label;
    this.description = description;
    this.properties = Collections.unmodifiableMap(properties);
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, ProvisionerProperty> getProperties() {
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

    ProvisionerSpecification that = (ProvisionerSpecification) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(label, that.label) &&
      Objects.equals(description, that.description) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, label, description, properties);
  }
}
