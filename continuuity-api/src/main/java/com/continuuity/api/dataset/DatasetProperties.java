package com.continuuity.api.dataset;

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

  /**
   * A Builder to construct DataSetSpecification instances.
   */
  public static final class Builder {
    private Map<String, String> properties = Maps.newHashMap();

    private Builder() {
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
     * Create a DataSetSpecification from this builder, using the private DataSetSpecification
     * constructor.
     * @return a complete DataSetSpecification
     */
    public DatasetProperties build() {
      return new DatasetProperties(this.properties);
    }
  }
}
