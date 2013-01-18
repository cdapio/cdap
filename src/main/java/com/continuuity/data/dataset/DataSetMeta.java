package com.continuuity.data.dataset;

import java.util.HashMap;
import java.util.Map;

/**
 * A DataSetMeta is a hierarchical meta data object that contains all
 * meta data needed to instantiate a data set at runtime. It is hierarchical
 * because it also contains the meta data for any embedded data sets that are
 * used in the implementation of the data set. DataSetMeta consists of:
 * <li>fixed meta fields such as the name and the type (the class) of a
 *   data set</li>
 * <li>custom string properties that vary from data set to data set,
 *   and that the data set implementation depends on</li>
 * <li>a DataSetMeta for each embedded data set. For instance,
 *   if a data set implements an indexed table using two base Tables,
 *   one for the data and one for the index, then these two tables their
 *   own meta data, which is carried along with the meta data for the
 *   indexed table.</li>
 * DataSetMeta uses a builder pattern for construction.
 */
public final class DataSetMeta {

  /** the name of the data set */
  private final String name;
  /** the class name of the data set */
  private final String type;
  /** the custom properties of the data set */
  private final Map<String, String> properties;
  /** the meta data for embedded data sets */
  private final Map<String, DataSetMeta> dataSetMetas;

  /** returns the name of the data set */
  public String getName() {
    return this.name;
  }

  /** returns the class name of the data set */
  public String getType() {
    return this.type;
  }

  /**
   * lookup a custom property of the data set
   * @param key the name of the property
   * @return the value of the property or null if the property does not exist
   */
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * get the meta data for an embedded data set
   * @param dsName the name of the embedded data set
   * @return the meta data for the named embedded data set,
   *    or null if not found.
   */
  public DataSetMeta getMetaFor(String dsName) {
    return dataSetMetas.get(dsName);
  }

  /** private constructor, only to be used by the builder */
  private DataSetMeta(String name, String type,
                      Map<String, String> properties,
                      Map<String, DataSetMeta> dataSetMetas) {
    this.name = name;
    this.type = type;
    this.properties = properties;
    this.dataSetMetas = dataSetMetas;
  }

  /** A Builder to construct DataSetMeta instances */
  public final static class Builder {
    // private fields
    private String name;
    private String type;
    private Map<String, String> properties
        = new HashMap<String, String>();
    private Map<String, DataSetMeta> dataSetMetas
        = new HashMap<String, DataSetMeta>();

    /**
     * Constructor from the data set that the meta data is for,
     * looks up and sets the name and the class of that data set.
     * @param dataset The data set for which a DataSetMeta is to be built
     */
    public Builder(DataSet dataset) {
      this.name = dataset.getName();
      this.type = dataset.getClass().getCanonicalName();
    }

    /**
     * Add a custom property
     * @param key the name of the custom property
     * @param value the value of the custom property
     * @return this builder object to allow chaining
     */
    public Builder property(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Add meta data for an embedded data set. Takes a builder and uses
     * that to create the DataSetMeta, then extracts the name from that meta
     * data.
     * @param builder a builder populated with all meta data
     * @return this builder object to allow chaining
     */
    public Builder dataset(DataSetMeta.Builder builder) {
      DataSetMeta meta = builder.create();
      this.dataSetMetas.put(meta.getName(), meta);
      return this;
    }

    /**
     * Create a DataSetMeta from this builder, using the private DataSetMeta
     * constructor.
     * @return a complete DataSetMeta
     */
    public DataSetMeta create() {
      return new DataSetMeta(this.name, this.type, this.properties,
          this.dataSetMetas);
    }
  }

}
