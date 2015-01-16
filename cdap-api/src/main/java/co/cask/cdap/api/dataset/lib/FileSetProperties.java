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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.dataset.DatasetProperties;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Helper to build properties for files datasets.
 */
public class FileSetProperties {

  /**
   * The base path of the dataset.
   */
  public static final String BASE_PATH = "base.path";

  /**
   * The name of the input format class.
   */
  public static final String INPUT_FORMAT = "input.format";

  /**
   * The name of the output format class.
   */
  public static final String OUTPUT_FORMAT = "output.format";

  /**
   * Prefix for additional properties for the input format. They are added to the
   * Hadoop configuration, with the prefix stripped from the name.
   */
  public static final String INPUT_PROPERTIES_PREFIX = "input.properties.";

  /**
   * Prefix for additional properties for the output format. They are added to the
   * Hadoop configuration, with the prefix stripped from the name.
   */
  public static final String OUTPUT_PROPERTIES_PREFIX = "output.properties.";

  /**
   * Whether this dataset should be enabled for explore.
   */
  public static final String PROPERTY_EXPLORE_ENABLED = "explore.enabled";

  /**
   * The serde to use for the Hive table.
   */
  public static final String PROPERTY_EXPLORE_SERDE = "explore.serde";

  /**
   * The input format to use for the Hive table.
   */
  public static final String PROPERTY_EXPLORE_INPUT_FORMAT = "explore.input.format";

  /**
   * The output format to use for the Hive table.
   */
  public static final String PROPERTY_EXPLORE_OUTPUT_FORMAT = "explore.output.format";

  /**
   * Prefix used to store additional table properties for Hive.
   */
  public static final String PROPERTY_EXPLORE_TABLE_PROPERTY_PREFIX = "explore.table.property.";

  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The base path configured in the properties.
   */
  public static String getBasePath(Map<String, String> properties) {
    return properties.get(BASE_PATH);
  }

  /**
   * @return The input format configured in the properties.
   */
  public static String getInputFormat(Map<String, String> properties) {
    return properties.get(INPUT_FORMAT);
  }

  /**
   * @return The output format configured in the properties.
   */
  public static String getOutputFormat(Map<String, String> properties) {
    return properties.get(OUTPUT_FORMAT);
  }

  /**
   * @return The input format properties configured in the properties.
   */
  public static Map<String, String> getInputProperties(Map<String, String> properties) {
    return propertiesWithPrefix(properties, INPUT_PROPERTIES_PREFIX);
  }

  /**
   * @return The output format properties configured in the properties.
   */
  public static Map<String, String> getOutputProperties(Map<String, String> properties) {
    return propertiesWithPrefix(properties, OUTPUT_PROPERTIES_PREFIX);
  }

  /**
   * @return whether explore is enabled by the properties.
   */
  public static boolean isExploreEnabled(Map<String, String> properties) {
    // Boolean.valueOf returns false if the value is null
    return Boolean.valueOf(properties.get(PROPERTY_EXPLORE_ENABLED));
  }

  /**
   * @return the class name of the serde configured in the properties.
   */
  public static String getSerdeClassName(Map<String, String> properties) {
    return properties.get(PROPERTY_EXPLORE_SERDE);
  }

  /**
   * @return the class name of the input format to be used in Hive.
   * Note that this can be different than the input format used
   * for the file set itself.
   */
  public static String getExploreInputFormat(Map<String, String> properties) {
    return properties.get(PROPERTY_EXPLORE_INPUT_FORMAT);
  }

  /**
   * @return the class name of the output format to be used in Hive.
   * Note that this can be different than the output format used
   * for the file set itself.
   */
  public static String getExploreOutputFormat(Map<String, String> properties) {
    return properties.get(PROPERTY_EXPLORE_OUTPUT_FORMAT);
  }

  /**
   * @return the Hive table properties configured in the properties.
   */
  public static Map<String, String> getTableProperties(Map<String, String> properties) {
    return propertiesWithPrefix(properties, PROPERTY_EXPLORE_TABLE_PROPERTY_PREFIX);
  }

  /**
   * @return a map of all properties whose key begins with the given prefix, without that prefix.
   */
  public static Map<String, String> propertiesWithPrefix(Map<String, String> properties, String prefix) {
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey().substring(prefix.length()), entry.getValue());
      }
    }
    return result;
  }

  /**
   * A Builder to construct properties for FileSet datasets.
   */
  public static class Builder {
    protected final DatasetProperties.Builder delegate = DatasetProperties.builder();

    /**
     * Package visible default constructor, to allow subclassing by other datasets in this package.
     */
    Builder() { }

    /**
     * Sets the base path for the file dataset.
     */
    public Builder setBasePath(String path) {
      delegate.add(BASE_PATH, path);
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setOutputFormat(Class<?> outputFormatClass) {
      delegate.add(OUTPUT_FORMAT, outputFormatClass.getName());
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setInputFormat(Class<?> inputFormatClass) {
      delegate.add(INPUT_FORMAT, inputFormatClass.getName());
      return this;
    }

    /**
     * Sets a property for the input format of the file dataset.
     */
    public Builder setInputProperty(String name, String value) {
      delegate.add(INPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }

    /**
     * Sets a property for the output format of the file dataset.
     */
    public Builder setOutputProperty(String name, String value) {
      delegate.add(OUTPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }

    /**
     * Enable explore for this dataset.
     */
    public Builder setExploreEnabled(boolean enabled) {
      delegate.add(PROPERTY_EXPLORE_ENABLED, Boolean.toString(enabled));
      return this;
    }

    /**
     * Set the class name of the SerDe used to create the Hive table.
     */
    public Builder setSerde(String className) {
      delegate.add(PROPERTY_EXPLORE_SERDE, className);
      return this;
    }

    /**
     * Set the class name of the SerDe used to create the Hive table.
     */
    public Builder setSerde(Class<?> serde) {
      return setSerde(serde.getName());
    }

    /**
     * Set the input format used to create the Hive table.
     * Note that this can be different than the input format used
     * for the file set itself.
     */
    public Builder setExploreInputFormat(String className) {
      delegate.add(PROPERTY_EXPLORE_INPUT_FORMAT, className);
      return this;
    }

    /**
     * Set the input format used to create the Hive table.
     * Note that this can be different than the input format used
     * for the file set itself.
     */
    public Builder setExploreInputFormat(Class<?> inputFormat) {
      return setExploreInputFormat(inputFormat.getName());
    }

    /**
     * Set the output format used to create the Hive table.
     * Note that this can be different than the output format used
     * for the file set itself.
     */
    public Builder setExploreOutputFormat(String className) {
      delegate.add(PROPERTY_EXPLORE_OUTPUT_FORMAT, className);
      return this;
    }

    /**
     * Set the output format used to create the Hive table.
     * Note that this can be different than the output format used
     * for the file set itself.
     */
    public Builder setExploreOutputFormat(Class<?> outputFormat) {
      return setExploreOutputFormat(outputFormat.getName());
    }

    /**
     * Set a table property to be added to the Hive table. Multiple properties can be set.
     */
    public Builder setTableProperty(String name, String value) {
      delegate.add(PROPERTY_EXPLORE_TABLE_PROPERTY_PREFIX + name, value);
      return this;
    }

    /**
     * Create a DatasetProperties from this builder.
     */
    public DatasetProperties build() {
      return delegate.build();
    }
  }
}
