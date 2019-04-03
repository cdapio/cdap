/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Dataset instance properties.
 */
public final class DatasetProperties {

  /**
   * Empty properties.
   */
  public static final DatasetProperties EMPTY = builder().build();

  /**
   * Schema property. Not all datasets support schema.
   */
  public static final String SCHEMA = "schema";

  private final String description;
  private final Map<String, String> properties;

  private DatasetProperties(Map<String, String> properties, @Nullable String description) {
    this.description = description;
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Helper method to create a DatasetProperties from a map of key/values.
   */
  public static DatasetProperties of(Map<String, String> props) {
    return builder().addAll(props).build();
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return properties of the dataset instance
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  @Override
  public String toString() {
    return "DatasetProperties{" +
      "description='" + description + '\'' +
      ", properties=" + properties +
      '}';
  }

  /**
   * A Builder to construct DatasetProperties instances.
   */
  public static class Builder {
    private String description;
    private Map<String, String> properties = new HashMap<>();

    protected Builder() {
    }

    /**
     * Sets description of the dataset
     *
     * @param description dataset description
     * @return this builder object to allow chaining
     */
    public Builder setDescription(String description) {
      this.description = description;
      return this;
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
     * Adds a property.
     * @param key the name of the property
     * @param value the value of the property
     * @return this builder object to allow chaining
     */
    public Builder add(String key, long value) {
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
      return new DatasetProperties(Collections.unmodifiableMap(this.properties), description);
    }
  }
}
