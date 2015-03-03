/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.dataset;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;

/**
 * Dataset instance properties.
 */
public final class DatasetProperties {

  /**
   * Empty properties.
   */
  public static final DatasetProperties EMPTY =
    new DatasetProperties(Collections.<String, String>emptyMap());

  /**
   * Schema property. Not all datasets support schema.
   */
  public static final String SCHEMA = "schema";

  private final Map<String, String> properties;

  private DatasetProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return properties of the dataset instance
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("properties", Joiner.on(",").withKeyValueSeparator("=").join(properties))
      .toString();
  }

  /**
   * A Builder to construct DatasetProperties instances.
   */
  public static class Builder {
    private Map<String, String> properties = Maps.newHashMap();

    protected Builder() {
    }

    /**
     * Adds a property.
     * @param key the name of the property
     * @param value the value of the property
     * @return this builder object to allow chaining
     */
    public Builder add(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Adds a property.
     * @param key the name of the property
     * @param value the value of the property
     * @return this builder object to allow chaining
     */
    public Builder add(String key, int value) {
      this.properties.put(key, String.valueOf(value));
      return this;
    }

    /**
     * Adds multiple properties.
     * @param properties the map of properties to add
     * @return this builder object to allow chaining
     */
    public Builder addAll(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }

    /**
     * Create a DatasetProperties from this builder, using the private DatasetProperties
     * constructor.
     */
    public DatasetProperties build() {
      return new DatasetProperties(this.properties);
    }
  }
}
