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

package io.cdap.cdap.api.dataset.lib;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.DatasetProperties;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper to build properties for files datasets.
 */
public class FileSetProperties {

  /**
   * The base path of the dataset.
   */
  public static final String BASE_PATH = "base.path";

  /**
   * Whether the files in the dataset are "external", that is, managed by an external process.
   */
  public static final String DATA_EXTERNAL = "data.external";

  /**
   * If true, the file set will use an existing directory as its base path and an existing Explore table.
   * When the file set is deleted or truncated, the directory and its contents as well as the Explore table
   * will not be deleted.
   */
  public static final String DATA_USE_EXISTING = "data.use.existing";

  /**
   * If true, the file set will use an existing directory as its base path and an existing Explore table,
   * and it will take possession of them.
   * When the file set is deleted or truncated, the directory and its contents as well as the Explore table
   * will be deleted, just as if they had been created by this file set.
   */
  public static final String DATA_POSSESS_EXISTING = "data.possess.existing";

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
   * The permissions for the dataset. The value for this property must be given either as a
   * 9-character String such as "rwxr-x---" or as an octal-base number such as 750. Permissions
   * will be applied by each dataset depending on the access control paradigm of the storage
   * engine.
   */
  @Beta
  public static final String PROPERTY_FILES_PERMISSIONS = "dataset.files.permissions";

  /**
   * The group name that the permission are assigned to. For file-based datasets, this group
   * name is used as the group for created files and directories; for table-based datasets,
   * group privileges will be granted to this group.
   */
  @Beta
  public static final String PROPERTY_FILES_GROUP = "dataset.files.group";

  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return the base path configured in the properties
   */
  public static String getBasePath(Map<String, String> properties) {
    return properties.get(BASE_PATH);
  }

  /**
   * @return the input format configured in the properties
   */
  public static String getInputFormat(Map<String, String> properties) {
    return properties.get(INPUT_FORMAT);
  }

  /**
   * @return whether the data (the files) in this dataset are considered external
   */
  public static boolean isDataExternal(Map<String, String> properties) {
    return Boolean.valueOf(properties.get(DATA_EXTERNAL));
  }

  /**
   * @return whether the file set is using a pre-existing base location
   */
  public static boolean isUseExisting(Map<String, String> properties) {
    return Boolean.valueOf(properties.get(DATA_USE_EXISTING));
  }

  /**
   * @return whether the file set is taking possession of a pre-existing base location
   */
  public static boolean isPossessExisting(Map<String, String> properties) {
    return Boolean.valueOf(properties.get(DATA_POSSESS_EXISTING));
  }

  /**
   * @return the output format configured in the properties
   */
  public static String getOutputFormat(Map<String, String> properties) {
    return properties.get(OUTPUT_FORMAT);
  }

  /**
   * @return the input format properties configured in the properties
   */
  public static Map<String, String> getInputProperties(Map<String, String> properties) {
    return propertiesWithPrefix(properties, INPUT_PROPERTIES_PREFIX);
  }

  /**
   * @return the output format properties configured in the properties
   */
  public static Map<String, String> getOutputProperties(Map<String, String> properties) {
    return propertiesWithPrefix(properties, OUTPUT_PROPERTIES_PREFIX);
  }

  /**
   * @return the default permissions for files and directories
   */
  @Beta
  public static String getFilePermissions(Map<String, String> properties) {
    return properties.get(PROPERTY_FILES_PERMISSIONS);
  }

  /**
   * @return the name of the group for files and directories
   */
  @Beta
  public static String getFileGroup(Map<String, String> properties) {
    return properties.get(PROPERTY_FILES_GROUP);
  }


  /**
   * @return a map of all properties whose key begins with the given prefix, without that prefix
   */
  public static Map<String, String> propertiesWithPrefix(Map<String, String> properties, String prefix) {
    return properties.entrySet().stream()
      .filter(entry -> entry.getKey().startsWith(prefix))
      .collect(Collectors.toMap(entry -> entry.getKey().substring(prefix.length()), Map.Entry::getValue, (a, b) -> b));
  }

  /**
   * A Builder to construct properties for FileSet datasets.
   */
  public static class Builder extends DatasetProperties.Builder {

    private String format;

    /**
     * Package visible default constructor, to allow sub-classing by other datasets in this package.
     */
    Builder() { }

    /**
     * Sets the base path for the file dataset.
     */
    public Builder setBasePath(String path) {
      add(BASE_PATH, path);
      return this;
    }

    /**
     * Configures whether the files (the data) in this fileset are managed externally.
     */
    public Builder setDataExternal(boolean isExternal) {
      add(DATA_EXTERNAL, Boolean.toString(isExternal));
      return this;
    }

    /**
     * Configures whether the file set should use an existing base location.
     */
    public Builder setUseExisting(boolean useExisting) {
      add(DATA_USE_EXISTING, Boolean.toString(useExisting));
      return this;
    }

    /**
     * Configures whether the file set should take possession of an existing base location.
     */
    public Builder setPossessExisting(boolean possessExisting) {
      add(DATA_POSSESS_EXISTING, Boolean.toString(possessExisting));
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setOutputFormat(Class<?> outputFormatClass) {
      setOutputFormat(outputFormatClass.getName());
      return this;
    }

    /**
     * Sets the output format of the file dataset.
     */
    public Builder setOutputFormat(String className) {
      add(OUTPUT_FORMAT, className);
      return this;
    }

    /**
     * Sets the input format of the file dataset.
     */
    public Builder setInputFormat(Class<?> inputFormatClass) {
      setInputFormat(inputFormatClass.getName());
      return this;
    }

    /**
     * Sets the input format of the file dataset.
     */
    public Builder setInputFormat(String className) {
      add(INPUT_FORMAT, className);
      return this;
    }

    /**
     * Sets a property for the input format of the file dataset.
     */
    public Builder setInputProperty(String name, String value) {
      add(INPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }

    /**
     * Sets a property for the output format of the file dataset.
     */
    public Builder setOutputProperty(String name, String value) {
      add(OUTPUT_PROPERTIES_PREFIX + name, value);
      return this;
    }
/**
     * Set the default permissions for files and directories
     */
    @Beta
    public Builder setFilePermissions(String permissions) {
      add(PROPERTY_FILES_PERMISSIONS, permissions);
      return this;
    }

    /**
     * Set the name of the group for files and directories
     */
    @Beta
    public Builder setFileGroup(String group) {
      add(PROPERTY_FILES_GROUP, group);
      return this;
    }

  }
}
