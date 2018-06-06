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

package co.cask.cdap.proto.provisioner;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Information about a provisioner name and properties.
 */
public class ProvisionerInfo {
  private final String name;
  private final Collection<ProvisionerPropertyValue> properties;

  public ProvisionerInfo(String name, Collection<ProvisionerPropertyValue> properties) {
    this.name = name;
    this.properties = Collections.unmodifiableList(
      properties.stream().filter(Objects::nonNull).collect(Collectors.toList()));
  }

  public String getName() {
    return name;
  }

  public Collection<ProvisionerPropertyValue> getProperties() {
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

    ProvisionerInfo that = (ProvisionerInfo) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties);
  }
}
