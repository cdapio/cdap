package com.continuuity.api.data;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.TreeMap;

/**
 * A DataSetSpecification is a hierarchical meta data object that contains all
 * meta data needed to instantiate a data set at runtime. It is hierarchical
 * because it also contains the specification for any embedded data sets that
 * are used in the implementation of the data set. DataSetSpecification
 * consists of:
 * <li>fixed fields such as the name and the type (the class) of a
 *   data set</li>
 * <li>custom string properties that vary from data set to data set,
 *   and that the data set implementation depends on</li>
 * <li>a DataSetSpecification for each embedded data set. For instance,
 *   if a data set implements an indexed table using two base Tables,
 *   one for the data and one for the index, then these two tables have
 *   their own spec, which must be carried along with the spec for the
 *   indexed table.</li>
 * DataSetSpecification uses a builder pattern for construction.
 */
public final class DataSetSpecification {

  // the name of the data set
  private final String name;
  // the class name of the data set
  private final String type;
  // the custom properties of the data set.
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final TreeMap<String, String> properties;
  // the meta data for embedded data sets
  // NOTE: we need the map to be ordered because we compare serialized to JSON form as Strings during deploy validation
  private final TreeMap<String, DataSetSpecification> dataSetSpecs;

  /**
   * Returns the name of the data set.
   * @return the name of the data set
   */
  public String getName() {
    return this.name;
  }

  /**
   * Returns the class name of the data set.
   * @return the class name of the data set
   */
  public String getType() {
    return this.type;
  }

  /**
   * Lookup a custom property of the data set.
   * @param key the name of the property
   * @return the value of the property or null if the property does not exist
   */
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Get the specification for an embedded data set.
   * @param dsName the name of the embedded data set
   * @return the specification for the named embedded data set,
   *    or null if not found.
   */
  public DataSetSpecification getSpecificationFor(String dsName) {
    return dataSetSpecs.get(dsName);
  }

  /**
   * Get specifications for all directly embedded data sets.
   * @return the iterable of specifications
   */
  public Iterable<DataSetSpecification> getSpecifications() {
    return ImmutableList.copyOf(dataSetSpecs.values());
  }


  /**
   * Private constructor, only to be used by the builder.
   * @param name the name of the data set
   * @param type the class name of the data set
   * @param properties the custom properties
   * @param dataSetSpecs the specs of embedded data sets
   */
  private DataSetSpecification(String name, String type,
                               TreeMap<String, String> properties,
                               TreeMap<String, DataSetSpecification> dataSetSpecs) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.dataSetSpecs = dataSetSpecs;
  }

  /**
   * Equality.
   */
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof DataSetSpecification)) {
      return false;
    }
    DataSetSpecification ds = (DataSetSpecification) other;
    return this.getName().equals(ds.getName())
        && this.getType().equals(ds.getType())
        && this.properties.equals(ds.properties)
        && this.dataSetSpecs.equals(ds.dataSetSpecs);
  }

  /**
   * Hash value.
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.type, this.properties, this.dataSetSpecs);
  }

  /**
   * A Builder to construct DataSetSpecification instances.
   */
  public static final class Builder {
    // private fields
    private String name;
    private String type;
    private TreeMap<String, String> properties
        = new TreeMap<String, String>();
    private TreeMap<String, DataSetSpecification> dataSetSpecs
        = new TreeMap<String, DataSetSpecification>();

    /**
     * Constructor from the data set that the specification is for,
     * looks up and sets the name and the class of that data set.
     * @param dataset The data set for which a DataSetSpecification is to be
     *                built
     */
    public Builder(DataSet dataset) {
      this.name = dataset.getName();
      this.type = dataset.getClass().getCanonicalName();
    }

    /**
     * Constructor from an existing data set spec. This allows adding
     * properties to an existing spec, for instance when extending an
     * existing data set class.
     * @param spec the existing data set spec
     */
    public Builder(DataSetSpecification spec) {
      this.name = spec.getName();
      this.type = spec.getType();
      this.properties = spec.properties;
      this.dataSetSpecs = spec.dataSetSpecs;
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
     * Add a specification for an embedded data set. Takes a builder and uses
     * that to create the DataSetSpecification, then extracts the name from
     * that specification.
     * @param spec a full data set spec for the embedded data set
     * @return this builder object to allow chaining
     */
    public Builder dataset(DataSetSpecification spec) {
      this.dataSetSpecs.put(spec.getName(), spec);
      return this;
    }

    /**
     * Create a DataSetSpecification from this builder, using the private DataSetSpecification
     * constructor.
     * @return a complete DataSetSpecification
     */
    public DataSetSpecification create() {
      return new DataSetSpecification(this.name, this.type, this.properties,
          this.dataSetSpecs);
    }
  }

}
