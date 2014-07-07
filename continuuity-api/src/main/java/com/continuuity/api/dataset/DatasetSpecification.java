package com.continuuity.api.dataset;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A {@link DatasetSpecification} is a hierarchical meta data object that contains all
 * meta data needed to instantiate a dataset at runtime. It is hierarchical
 * because it also contains the specification for any underlying datasets that
 * are used in the implementation of the dataset. {@link DatasetSpecification}
 * consists of:
 * <li>fixed fields such as the dataset instance name and the dataset type name</li>
 * <li>custom string properties that vary from dataset to dataset,
 *   and that the dataset implementation depends on</li>
 * <li>a {@link DatasetSpecification} for each underlying dataset. For instance,
 *   if a dataset implements an indexed table using two base Tables,
 *   one for the data and one for the index, then these two tables have
 *   their own spec, which must be carried along with the spec for the
 *   indexed table.</li>
 * {@link DatasetSpecification} uses a builder pattern for construction.
 */
public final class DatasetSpecification {

  // the name of the dataset
  private final String name;
  // the name of the type of the dataset
  private final String type;
  // the custom properties of the dataset.
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final SortedMap<String, String> properties;
  // the meta data for embedded datasets
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final SortedMap<String, DatasetSpecification> datasetSpecs;

  public static Builder builder(String name, String typeName) {
    return new Builder(name, typeName);
  }

  public static DatasetSpecification changeName(DatasetSpecification spec, String newName) {
    return new DatasetSpecification(newName, spec.type,
                                    spec.properties, spec.datasetSpecs);
  }

  /**
   * Private constructor, only to be used by the builder.
   * @param name the name of the dataset
   * @param type the type of the dataset
   * @param properties the custom properties
   * @param datasetSpecs the specs of embedded datasets
   */
  private DatasetSpecification(String name,
                               String type,
                               SortedMap<String, String> properties,
                               SortedMap<String, DatasetSpecification> datasetSpecs) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.datasetSpecs = datasetSpecs;
  }

  /**
   * Returns the name of the dataset.
   * @return the name of the dataset
   */
  public String getName() {
    return this.name;
  }

  /**
   * Returns the type of the dataset.
   * @return the type of the dataset
   */
  public String getType() {
    return this.type;
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @return the value of the property or null if the property does not exist
   */
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @param defaultValue the value to return if property does not exist
   * @return the value of the property or defaultValue if the property does not exist
   */
  public String getProperty(String key, String defaultValue) {
    return properties.containsKey(key) ? getProperty(key) : defaultValue;
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @param defaultValue the value to return if property does not exist
   * @return the value of the property or defaultValue if the property does not exist
   */
  public long getLongProperty(String key, long defaultValue) {
    return properties.containsKey(key) ? Long.parseLong(getProperty(key)) : defaultValue;
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @param defaultValue the value to return if property does not exist
   * @return the value of the property or defaultValue if the property does not exist
   */
  public int getIntProperty(String key, int defaultValue) {
    return properties.containsKey(key) ? Integer.parseInt(getProperty(key)) : defaultValue;
  }

  /**
   * Return map of all properties set in this specification.
   * @return an immutable map.
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  /**
   * Get the specification for an embedded dataset.
   * @param dsName the name of the embedded dataset
   * @return the specification for the named embedded dataset,
   *    or null if not found.
   */
  public DatasetSpecification getSpecification(String dsName) {
    return datasetSpecs.get(dsName);
  }

  /**
   * Equality.
   */
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof DatasetSpecification)) {
      return false;
    }
    DatasetSpecification ds = (DatasetSpecification) other;
    return this.getName().equals(ds.getName())
        && this.type.equals(ds.type)
        && this.properties.equals(ds.properties)
        && this.datasetSpecs.equals(ds.datasetSpecs);
  }

  /**
   * Hash value.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.type, this.properties, this.datasetSpecs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("type", type)
      .add("properties", Joiner.on(",").withKeyValueSeparator("=").join(properties))
      .add("datasetSpecs", Joiner.on(",").withKeyValueSeparator("=").join(datasetSpecs))
      .toString();
  }

  /**
   * A Builder to construct DataSetSpecification instances.
   */
  public static final class Builder {
    // private fields
    private final String name;
    private final String type;
    private final TreeMap<String, String> properties;
    private final TreeMap<String, DatasetSpecification> dataSetSpecs;

    private Builder(String name, String typeName) {
      this.name = name;
      this.type = typeName;
      this.properties = Maps.newTreeMap();
      this.dataSetSpecs = Maps.newTreeMap();
    }

    /**
     * Add underlying dataset specs.
     * @param specs specs to add
     * @return this builder object to allow chaining
     */
    public Builder datasets(DatasetSpecification... specs) {
      return datasets(Lists.newArrayList(specs));
    }

    /**
     * Add underlying dataset specs.
     * @param specs specs to add
     * @return this builder object to allow chaining
     */
    public Builder datasets(Collection<? extends DatasetSpecification> specs) {
      for (DatasetSpecification spec : specs) {
        this.dataSetSpecs.put(spec.getName(), spec);
      }
      return this;
    }

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
     * Add properties.
     * @param props properties to add
     * @return this builder object to allow chaining
     */
    public Builder properties(Map<String, String> props) {
      this.properties.putAll(props);
      return this;
    }

    /**
     * Create a DataSetSpecification from this builder, using the private DataSetSpecification
     * constructor.
     * @return a complete DataSetSpecification
     */
    public DatasetSpecification build() {
      return namespace(new DatasetSpecification(this.name, this.type, this.properties, this.dataSetSpecs));
    }

    /**
     * Prefixes all DataSets embedded inside the given {@link DatasetSpecification} with the name of the enclosing
     * DataSet.
     */
    private DatasetSpecification namespace(DatasetSpecification spec) {
      return namespace(null, spec);
    }

    /*
     * Prefixes all DataSets embedded inside the given {@link DataSetSpecification} with the given namespace.
     */
    private DatasetSpecification namespace(String namespace, DatasetSpecification spec) {
      // Name of the DataSetSpecification is prefixed with namespace if namespace is present.
      String name;
      if (namespace == null) {
        name = spec.getName();
      } else {
        name = namespace;
        if (!spec.getName().isEmpty()) {
          name += '.' + spec.getName();
        }
      }

      // If no namespace is given, starts with using the DataSet name.
      namespace = (namespace == null) ? spec.getName() : namespace;

      TreeMap<String, DatasetSpecification> specifications = Maps.newTreeMap();
      for (Map.Entry<String, DatasetSpecification> entry : spec.datasetSpecs.entrySet()) {
        specifications.put(entry.getKey(), namespace(namespace, entry.getValue()));
      }

      return new DatasetSpecification(name, type, spec.properties, specifications);
    }
  }
}
