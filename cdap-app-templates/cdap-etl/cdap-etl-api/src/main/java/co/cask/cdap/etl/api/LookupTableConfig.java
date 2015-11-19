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

import com.google.common.collect.ImmutableMap;

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

  private final TableType type;

  private final Map<String, String> datasetProperties;
  private final CacheConfig cacheConfig;
  private final boolean cacheEnabled;

  /**
   * @param type type of lookup table
   * @param cacheConfig cache config
   * @param datasetProperties runtime dataset properties
   * @param cacheEnabled true if caching is desired
   */
  public LookupTableConfig(TableType type, CacheConfig cacheConfig,
                           Map<String, String> datasetProperties, boolean cacheEnabled) {
    this.type = type;
    this.cacheConfig = cacheConfig;
    this.datasetProperties = datasetProperties;
    this.cacheEnabled = cacheEnabled;
  }

  /**
   * @param type type of lookup table
   */
  public LookupTableConfig(TableType type) {
    this(type, new CacheConfig(), ImmutableMap.<String, String>of(), false);
  }

  public TableType getType() {
    return type;
  }

  public Map<String, String> getDatasetProperties() {
    return datasetProperties;
  }

  public boolean isCacheEnabled() {
    return cacheEnabled;
  }

  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }
}
