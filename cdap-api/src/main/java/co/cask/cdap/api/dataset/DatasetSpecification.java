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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;

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
  // description of dataset
  private final String description;
  // the properties of the dataset as passed in when the dataset was created or reconfigured
  private final Map<String, String> originalProperties;
  // the custom properties of the dataset.
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final SortedMap<String, String> properties;
  // the meta data for embedded datasets
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final SortedMap<String, DatasetSpecification> datasetSpecs;

  public static Builder builder(String name, String typeName) {
    return new Builder(name, typeName);
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
                               @Nullable String description,
                               SortedMap<String, String> properties,
                               SortedMap<String, DatasetSpecification> datasetSpecs) {
    this(name, type, description, null, properties, datasetSpecs);
  }

  /**
   * Private constructor, only to be used by static method setOriginalProperties.
   * @param name the name of the dataset
   * @param type the type of the dataset
   * @param description the description of dataset
   * @param properties the custom properties
   * @param datasetSpecs the specs of embedded datasets
   */
  private DatasetSpecification(String name,
                               String type,
                               @Nullable String description,
                               @Nullable Map<String, String> originalProperties,
                               SortedMap<String, String> properties,
                               SortedMap<String, DatasetSpecification> datasetSpecs) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.properties = Collections.unmodifiableSortedMap(new TreeMap<>(properties));
    this.originalProperties = originalProperties == null ? null :
      Collections.unmodifiableMap(new TreeMap<>(originalProperties));
    this.datasetSpecs = Collections.unmodifiableSortedMap(new TreeMap<>(datasetSpecs));
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
   * Returns the description of dataset.
   * @return the description of dataset
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @return the value of the property or {@code null} if the property does not exist
   */
  @Nullable
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
    return properties.containsKey(key) ? Long.parseLong(properties.get(key)) : defaultValue;
  }

  /**
   * Lookup a custom property of the dataset.
   * @param key the name of the property
   * @param defaultValue the value to return if property does not exist
   * @return the value of the property or defaultValue if the property does not exist
   */
  public int getIntProperty(String key, int defaultValue) {
    return properties.containsKey(key) ? Integer.parseInt(properties.get(key)) : defaultValue;
  }

  /**
   * Return the original properties with which the dataset was created/reconfigured.
   * @return an immutable map. For embedded datasets, this will always return null.
   */
  @Nullable
  public Map<String, String> getOriginalProperties() {
    return originalProperties;
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
   * Get the map of embedded dataset name to {@link co.cask.cdap.api.dataset.DatasetSpecification}
   * @return the map of dataset name to {@link co.cask.cdap.api.dataset.DatasetSpecification}
   */
  public SortedMap<String, DatasetSpecification> getSpecifications() {
    return datasetSpecs;
  }

  /**
   * @return a new spec that is the same as this one, with the original properties set to the provided properties.
   */
  public DatasetSpecification setOriginalProperties(DatasetProperties originalProps) {
    return setOriginalProperties(originalProps.getProperties());
  }

  /**
   * @return a new spec that is the same as this one, with the original properties set to the provided properties.
   */
  public DatasetSpecification setOriginalProperties(Map<String, String> originalProps) {
    return new DatasetSpecification(name, type, description, originalProps, properties, datasetSpecs);
  }

  public DatasetSpecification setDescription(String description) {
    return new DatasetSpecification(name, type, description, originalProperties, properties, datasetSpecs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetSpecification that = (DatasetSpecification) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(description, that.description) &&
      Objects.equals(originalProperties, that.originalProperties) &&
      Objects.equals(properties, that.properties) &&
      Objects.equals(datasetSpecs, that.datasetSpecs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, description, originalProperties, properties, datasetSpecs);
  }

  /**
   * Returns true if the datasetName is the name of a non-composite dataset represented by this spec.
   * That is, it is represented by one of the leaf-nodes of this dataset spec.
   * @param datasetName the name of a dataset
   * @return <code>true</code> if the datasetName is represented by the dataset spec;
   *         <code>false</code> otherwise
   */
  public boolean isParent(String datasetName) {
    return isParent(datasetName, this);
  }

  private boolean isParent(String datasetName, DatasetSpecification specification) {
    if (datasetName == null) {
      return false;
    }
    if (specification.getSpecifications().size() == 0 && specification.getName().equals(datasetName)) {
      return true;
    }
    if (datasetName.startsWith(specification.getName())) {
      for (DatasetSpecification spec : specification.getSpecifications().values()) {
        if (isParent(datasetName, spec)) {
          return true;
        }
      }
    }
    return  false;
  }

  @Override
  public String toString() {
    return "DatasetSpecification{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", description='" + description + '\'' +
      ", originalProperties=" + originalProperties +
      ", properties=" + properties +
      ", datasetSpecs=" + datasetSpecs +
      '}';
  }

  /**
   * A Builder to construct DatasetSpecification instances.
   */
  public static final class Builder {
    // private fields
    private final String name;
    private final String type;
    private String description;
    private final TreeMap<String, String> properties;
    private final TreeMap<String, DatasetSpecification> dataSetSpecs;

    private Builder(String name, String typeName) {
      this.name = name;
      this.type = typeName;
      this.properties = new TreeMap<>();
      this.dataSetSpecs = new TreeMap<>();
    }

    /**
     * Set the dataset description.
     * This description will be overridden by the description set during dataset instance creation.
     *
     * @param description dataset description
     * @return this builder object to allow chaining
     */
    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }

    /**
     * Add underlying dataset specs.
     * @param specs specs to add
     * @return this builder object to allow chaining
     */
    public Builder datasets(DatasetSpecification... specs) {
      return datasets(Arrays.asList(specs));
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
      return namespace(new DatasetSpecification(name, type, description, properties, dataSetSpecs));
    }

    /**
     * Prefixes all DataSets embedded inside the given {@link DatasetSpecification} with the name of the enclosing
     * Dataset.
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

      TreeMap<String, DatasetSpecification> specifications = new TreeMap<>();
      for (Map.Entry<String, DatasetSpecification> entry : spec.datasetSpecs.entrySet()) {
        specifications.put(entry.getKey(), namespace(namespace, entry.getValue()));
      }

      return new DatasetSpecification(name, spec.type, spec.description, spec.properties, specifications);
    }
  }
}
