/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Beta;
import com.google.common.base.Joiner;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helper to build properties for files datasets.
 */
@Beta
public class PartitionedFileSetProperties extends FileSetProperties {

  /**
   * The property name for the list of partitioning field names.
   */
  public static final String PARTITIONING_FIELDS = "partitioning.fields.";

  /**
   * The prefix for fields of the partitioning.
   */
  public static final String PARTITIONING_FIELD_PREFIX = "partitioning.field.";

  /**
   * Read the partitioning for a PartitionedFileSet from its properties.
   *
   * @param properties the dataset properties
   * @return the partitioning found in the properties, or null if the properties contain no partitioning.
   */
  @Nullable
  public static Partitioning getPartitioning(Map<String, String> properties) {
    String fieldList = properties.get(PARTITIONING_FIELDS);
    if (null == fieldList) {
      return null;
    }
    String[] fieldNames = fieldList.split(",");
    if (fieldNames.length == 0) {
      return null;
    }
    Partitioning.Builder builder = Partitioning.builder();
    for (String fieldName : fieldNames) {
      String typeString = properties.get(PARTITIONING_FIELD_PREFIX + fieldName);
      if (null == typeString) {
        throw new IllegalArgumentException(String.format("Type of field '%s' is missing", fieldName));
      }
      try {
        Partitioning.FieldType fieldType = Partitioning.FieldType.valueOf(typeString);
        builder.addField(fieldName, fieldType);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          String.format("Type of field '%s' is invalid: '%s'", fieldName, typeString), e);
      }
    }
    return builder.build();
  }

  /**
   * @return a properties builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A Builder to construct properties for FileSet datasets.
   */
  public static class Builder extends FileSetProperties.Builder {

    /**
     * Package visible default constructor, to allow sub-classing by other datasets in this package.
     */
    Builder() { }

    /**
     * Sets the base path for the file dataset.
     */
    public Builder setPartitioning(Partitioning partitioning) {
      delegate.add(PARTITIONING_FIELDS, Joiner.on(",").join(partitioning.getFields().keySet()));
      for (Map.Entry<String, Partitioning.FieldType> entry : partitioning.getFields().entrySet()) {
        delegate.add(PARTITIONING_FIELD_PREFIX + entry.getKey(), entry.getValue().name());
      }
      return this;
    }
  }
}
