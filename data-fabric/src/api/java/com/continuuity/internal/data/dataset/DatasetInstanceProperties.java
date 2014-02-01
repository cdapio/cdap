package com.continuuity.internal.data.dataset;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;

/**
 * Dataset instance properties.
 *
 * This may include properties of the underlying datasets if any to allow finer configuration of them.
 */
public final class DatasetInstanceProperties {

  /**
   * Empty properties
   */
  public static final DatasetInstanceProperties EMPTY =
    new DatasetInstanceProperties();

  private final SortedMap<String, String> properties;
  private final SortedMap<String, DatasetInstanceProperties> underlying;

  private DatasetInstanceProperties() {
    this.properties = Maps.newTreeMap();
    this.underlying = Maps.newTreeMap();
  }

  private DatasetInstanceProperties(Map<String, String> properties,
                                    Map<String, DatasetInstanceProperties> underlyingDatasetsProperties) {
    this.properties = Maps.newTreeMap();
    this.properties.putAll(properties);
    this.underlying = Maps.newTreeMap();
    this.underlying.putAll(underlyingDatasetsProperties);
  }

  /**
   * @param datasetInstanceName name of the dataset instance to get properties for
   * @return properties of the underlying dataset instance
   */
  public DatasetInstanceProperties getProperties(String datasetInstanceName) {
    DatasetInstanceProperties props = underlying.get(datasetInstanceName);
    return props == null ? DatasetInstanceProperties.EMPTY : props;
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
    private SortedMap<String, String> properties = Maps.newTreeMap();
    private SortedMap<String, DatasetInstanceProperties> underlying = Maps.newTreeMap();

    /**
     * Add a custom property.
     * @param key the name of the custom property
     * @param value the value of the custom property
     * @return this builder object to allow chaining
     */
    public Builder property(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Add a properties of underlying dataset instance.
     * @param instanceName the name of the underlying dataset instance
     * @param props properties to set
     * @return this builder object to allow chaining
     */
    public Builder property(String instanceName, DatasetInstanceProperties props) {
      this.underlying.put(instanceName, props);
      return this;
    }

    /**
     * Create a DataSetSpecification from this builder, using the private DataSetSpecification
     * constructor.
     * @return a complete DataSetSpecification
     */
    public DatasetInstanceProperties build() {
      return new DatasetInstanceProperties(this.properties, this.underlying);
    }
  }
}
