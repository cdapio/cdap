/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.connector;

import java.util.Map;
import java.util.Objects;

/**
 * Plugin spec on what plugins are related to the connector
 */
public class PluginSpec {

  private final String name;
  private final String type;
  private final Map<String, String> properties;

  public PluginSpec(String name, String type, Map<String, String> properties) {
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

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginSpec that = (PluginSpec) o;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, properties);
  }
}
