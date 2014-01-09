package com.continuuity.api.data.dataset2;

import com.continuuity.api.common.PropertyProvider;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * A {@link DatasetInstanceSpec} is a hierarchical meta data object that contains all
 * meta data needed to instantiate a dataset at runtime. It is hierarchical
 * because it also contains the specification for any underlying datasets that
 * are used in the implementation of the data set. {@link DatasetInstanceSpec}
 * consists of:
 * <li>fixed fields such as the dataset instance name and the dataset type name</li>
 * <li>custom string properties that vary from dataset to dataset,
 *   and that the dataset implementation depends on</li>
 * <li>a {@link DatasetInstanceSpec} for each underlying dataset. For instance,
 *   if a dataset implements an indexed table using two base Tables,
 *   one for the data and one for the index, then these two tables have
 *   their own spec, which must be carried along with the spec for the
 *   indexed table.</li>
 * {@link DatasetInstanceSpec} uses a builder pattern for construction.
 */
public final class DatasetInstanceSpec implements PropertyProvider {

  // the name of the data set
  private final String name;
  // the type of the data set
  private final String type;
  // the custom properties of the data set.
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final TreeMap<String, String> properties;
  // the meta data for embedded data sets
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final TreeMap<String, DatasetInstanceSpec> datasetSpecs;

  /**
   * Private constructor, only to be used by the builder.
   * @param name the name of the data set
   * @param type the type of the data set
   * @param properties the custom properties
   * @param datasetSpecs the specs of embedded data sets
   */
  private DatasetInstanceSpec(String name,
                              String type,
                              TreeMap<String, String> properties,
                              TreeMap<String, DatasetInstanceSpec> datasetSpecs) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.datasetSpecs = datasetSpecs;
  }

  /**
   * Returns the name of the data set.
   * @return the name of the data set
   */
  public String getName() {
    return this.name;
  }

  /**
   * Returns the type of the data set.
   * @return the type of the data set
   */
  public String getType() {
    return this.type;
  }

  /**
   * Lookup a custom property of the data set.
   * @param key the name of the property
   * @return the value of the property or null if the property does not exist
   */
  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Lookup a custom property of the data set.
   * @param key the name of the property
   * @param defaultValue the value to return if property does not exist
   * @return the value of the property or defaultValue if the property does not exist
   */
  public String getProperty(String key, String defaultValue) {
    String value = getProperty(key);
    return value == null ? defaultValue : value;
  }

  /**
   * Return map of all properties set in this specification.
   * @return an immutable map.
   */
  @Override
  public Map<String, String> getProperties() {
    return ImmutableMap.copyOf(properties);
  }

  /**
   * Get the specification for an embedded data set.
   * @param dsName the name of the embedded data set
   * @return the specification for the named embedded data set,
   *    or null if not found.
   */
  public DatasetInstanceSpec getSpecificationFor(String dsName) {
    return datasetSpecs.get(dsName);
  }

  /**
   * Get specifications for all directly embedded data sets.
   * @return the iterable of specifications
   */
  public Iterable<DatasetInstanceSpec> getSpecifications() {
    return ImmutableList.copyOf(datasetSpecs.values());
  }

  /**
   * Equality.
   */
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof DatasetInstanceSpec)) {
      return false;
    }
    DatasetInstanceSpec ds = (DatasetInstanceSpec) other;
    return this.getName().equals(ds.getName())
        && this.properties.equals(ds.properties)
        && this.datasetSpecs.equals(ds.datasetSpecs);
  }

  /**
   * Hash value.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.properties, this.datasetSpecs);
  }

  /**
   * A Builder to construct DataSetSpecification instances.
   */
  public static final class Builder {
    // private fields
    private final String name;
    private final String type;
    private final TreeMap<String, String> properties;
    private final TreeMap<String, DatasetInstanceSpec> dataSetSpecs;

    public Builder(String name, String typeName) {
      this.name = name;
      this.type = typeName;
      this.properties = new TreeMap<String, String>();
      this.dataSetSpecs = new TreeMap<String, DatasetInstanceSpec>();
    }

    /**
     * Add underlying dataset specs
     * @param specs specs to add
     * @return this builder object to allow chaining
     */
    public Builder datasets(DatasetInstanceSpec... specs) {
      return datasets(Lists.newArrayList(specs));
    }

    /**
     * Add underlying dataset specs
     * @param specs specs to add
     * @return this builder object to allow chaining
     */
    public Builder datasets(Collection<DatasetInstanceSpec> specs) {
      for (DatasetInstanceSpec spec : specs) {
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
     * Create a DataSetSpecification from this builder, using the private DataSetSpecification
     * constructor.
     * @return a complete DataSetSpecification
     */
    public DatasetInstanceSpec create() {
      return namespace(new DatasetInstanceSpec(this.name, this.type, this.properties, this.dataSetSpecs));
    }

    /**
     * Prefixes all DataSets embedded inside the given {@link DatasetInstanceSpec} with the name of the enclosing
     * DataSet.
     */
    private DatasetInstanceSpec namespace(DatasetInstanceSpec spec) {
      return namespace(null, spec);
    }

    /*
     * Prefixes all DataSets embedded inside the given {@link DataSetSpecification} with the given namespace.
     */
    private DatasetInstanceSpec namespace(String namespace, DatasetInstanceSpec spec) {
      // Name of the DataSetSpecification is prefixed with namespace if namespace is present.
      String name = (namespace == null) ? spec.getName() : namespace + '.' + spec.getName();
      // If no namespace is given, starts with using the DataSet name.
      namespace = (namespace == null) ? spec.getName() : namespace;

      TreeMap<String, DatasetInstanceSpec> specifications = Maps.newTreeMap();
      for (Map.Entry<String, DatasetInstanceSpec> entry : spec.datasetSpecs.entrySet()) {
        specifications.put(entry.getKey(), namespace(namespace, entry.getValue()));
      }

      return new DatasetInstanceSpec(name, type, spec.properties, specifications);
    }

    // todo
    public Builder properties(DatasetInstanceProperties properties) {
      // todo: write properties to corresponded dataset instance specs
      return this;
    }
  }
}
