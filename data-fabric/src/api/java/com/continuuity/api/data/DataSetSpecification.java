package com.continuuity.api.data;

import com.continuuity.api.annotation.Property;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.internal.Primitives;

import java.lang.reflect.Field;
import java.util.Map;
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
      this.type = dataset.getClass().getName();

      try {
        // Inspect all fields of the DataSet in the class hierarchy.
        for (TypeToken<?> type : TypeToken.of(dataset.getClass()).getTypes().classes()) {
          Class<?> clz = type.getRawType();

          if (Object.class.equals(clz)) {
            break;
          }

          for (Field field : clz.getDeclaredFields()) {
            if (DataSet.class.isAssignableFrom(field.getType())) {
              // For field of DataSet type, call DataSet.configure() and store it.
              addDataSetSpecification(dataset, field);

            } else if (field.isAnnotationPresent(Property.class)) {
              // For @Property field, store it in properties if it is primitive type (boxed as well) or String.
              addProperty(dataset, field);
            }
          }
        }
      } catch (IllegalAccessException e) {
        throw Throwables.propagate(e);
      }
    }

    /**
     * Add embedded data sets
     * @param dataSet A {@link DataSet} to add.
     * @param moreDataSet List of {@link DataSet} to add.
     * @return this builder object to allow chaining
     */
    public Builder datasets(DataSet dataSet, DataSet...moreDataSet) {
      return datasets(ImmutableList.<DataSet>builder().add(dataSet).add(moreDataSet).build());
    }

    /**
     * Add a list of embedded data sets
     * @param dataSets An {@link Iterable} of {@link DataSet} to add.
     * @return this builder object to allow chaining.
     */
    public Builder datasets(Iterable<DataSet> dataSets) {
      for (DataSet dataSet : dataSets) {
        DataSetSpecification spec = dataSet.configure();
        // Prefix the key with "." to avoid name collision with field based DataSets.
        String key = "." + spec.getName();
        if (this.dataSetSpecs.containsKey(key)) {
          Preconditions.checkArgument(spec.equals(this.dataSetSpecs.get(key)),
                                      "DataSet '%s' already added with different specification.", spec.getName());
        } else {
          this.dataSetSpecs.put("." + spec.getName(), spec);
        }
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
    public DataSetSpecification create() {
      return namespace(new DataSetSpecification(this.name, this.type, this.properties, this.dataSetSpecs));
    }

    /**
     * Adds the DataSetSpecification of a DataSet field in the given DataSet instance.
     */
    private void addDataSetSpecification(DataSet dataset, Field field) throws IllegalAccessException {
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }

      DataSetSpecification specification = ((DataSet) field.get(dataset)).configure();
      // Key to DataSetSpecification is "className.fieldName" to avoid name collision.
      String key = field.getDeclaringClass().getName() + '.' + field.getName();
      dataSetSpecs.put(key, specification);
    }

    /**
     * Adds the value of a field in the given DataSet instance.
     */
    private void addProperty(DataSet dataset, Field field) throws IllegalAccessException {
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }
      Class<?> fieldType = field.getType();

      // Only support primitive type, boxed type, String and Enum
      Preconditions.checkArgument(
        fieldType.isPrimitive() || Primitives.isWrapperType(fieldType) ||
        String.class.equals(fieldType) || fieldType.isEnum(),
        "Unsupported property type %s of field %s in class %s.",
        fieldType.getName(), field.getName(), field.getDeclaringClass().getName());

      Object value = field.get(dataset);
      if (value == null) {
        // Not storing null field.
        return;
      }

      // Key name is "className.fieldName".
      String key = field.getDeclaringClass().getName() + '.' + field.getName();
      properties.put(key, fieldType.isEnum() ? ((Enum<?>) value).name() : value.toString());
    }

    /**
     * Prefixes all DataSets embedded inside the given {@link DataSetSpecification} with the name of the enclosing
     * DataSet.
     */
    private DataSetSpecification namespace(DataSetSpecification spec) {
      return namespace(null, spec);
    }


    /*
     * Prefixes all DataSets embedded inside the given {@link DataSetSpecification} with the given namespace.
     */
    private DataSetSpecification namespace(String namespace, DataSetSpecification spec) {
      // Name of the DataSetSpecification is prefixed with namespace if namespace is present.
      String name = (namespace == null) ? spec.getName() : namespace + '.' + spec.getName();
      // If no namespace is given, starts with using the DataSet name.
      namespace = (namespace == null) ? spec.getName() : namespace;

      TreeMap<String, DataSetSpecification> specifications = Maps.newTreeMap();
      for (Map.Entry<String, DataSetSpecification> entry : spec.dataSetSpecs.entrySet()) {
        specifications.put(entry.getKey(), namespace(namespace, entry.getValue()));
      }

      return new DataSetSpecification(name, spec.getType(), spec.properties, specifications);
    }
  }
}
