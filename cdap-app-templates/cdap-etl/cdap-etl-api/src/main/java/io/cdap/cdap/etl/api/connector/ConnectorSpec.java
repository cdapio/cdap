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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The connector spec contains all the properties based on the path and plugin config
 */
public class ConnectorSpec {
  private final Map<String, String> properties;

  private ConnectorSpec(Map<String, String> properties) {
    this.properties = properties;
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

    ConnectorSpec that = (ConnectorSpec) o;
    return Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties);
  }

  /**
   * Get the builder to build this object
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link BrowseEntityProperty}
   */
  public static class Builder {
    private Map<String, String> properties;

    public Builder() {
      this.properties = new HashMap<>();
    }

    public Builder setProperties(Map<String, String> properties) {
      this.properties.clear();
      this.properties.putAll(properties);
      return this;
    }

    public Builder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public Builder addProperties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public ConnectorSpec build() {
      return new ConnectorSpec(properties);
    }
  }
}
