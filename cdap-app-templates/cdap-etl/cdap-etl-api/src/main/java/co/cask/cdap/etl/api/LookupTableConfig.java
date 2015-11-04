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
package co.cask.cdap.etl.api;

import co.cask.cdap.api.dataset.DatasetProperties;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Configuration for a particular {@link Lookup} table.
 */
public class LookupTableConfig {

  /**
   * Type of lookup table.
   */
  public enum TableType {
    DATASET
  }

  /**
   * When type is DATASET, {@link #properties} is interpreted as {@link DatasetProperties}.
   */
  private final TableType type;
  private final Map<String, Object> properties;

  public LookupTableConfig(TableType type, Map<String, Object> properties) {
    this.type = type;
    this.properties = properties;
  }

  public TableType getType() {
    return type;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public DatasetProperties getDatasetProperties() {
    Preconditions.checkArgument(type == TableType.DATASET);

    Object argumentsObj = properties.get("arguments");
    if (argumentsObj == null) {
      return DatasetProperties.EMPTY;
    }

    if (!(argumentsObj instanceof Map)) {
      throw new IllegalArgumentException("Expected 'arguments' property to be a map of string to string");
    }

    @SuppressWarnings("unchecked")
    Map<String, String> arguments = (Map<String, String>) argumentsObj;
    return DatasetProperties.builder()
      .addAll(arguments)
      .build();
  }
}
