/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner.compression;

import io.cdap.cdap.storage.spanner.SpannerStorageProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages compression configurations for the Spanner database.
 */
public class DatabaseCompressionConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseCompressionConfig.class);

  private final Map<String, Map<String, CompressionConfig>> configs;

  private DatabaseCompressionConfig(@Nonnull Map<String, Map<String, CompressionConfig>> configs) {
    this.configs = Collections.unmodifiableMap(configs);
  }

  /**
   * Parses the compression configuration string and creates a DatabaseCompressionConfig instance.
   * The expected format is a comma-separated list of table:column:compressor, where table is the
   * table name, column is the column name, and compressor is the compressor type (e.g.,
   * "table_1:column_1:SNAPPY").
   *
   * @param compressionConfig compression config string fetched from {@link SpannerStorageProvider}
   * @return parsed DatabaseCompressionConfig or empty configuration if the input is null or empty.
   * @throws IllegalArgumentException if the compression configuration string has an invalid
   *                                  format.
   */
  public static DatabaseCompressionConfig parse(String compressionConfig) {
    Map<String, Map<String, CompressionConfig>> tableCompressionConfigs = new HashMap<>();

    if (compressionConfig == null || compressionConfig.isEmpty()) {
      LOG.warn("Spanner is being used without compression.");
      return new DatabaseCompressionConfig(tableCompressionConfigs);
    }

    String[] configs = compressionConfig.split(",");
    for (String config : configs) {
      String[] parts = config.split(":");
      if (parts.length == 3) {
        String tableName = parts[0];
        String columnName = parts[1];
        CompressorType compressorType = CompressorType.fromString(parts[2]);
        tableCompressionConfigs.computeIfAbsent(tableName, k -> new HashMap<>())
            .put(columnName, new CompressionConfig(compressorType));
      } else {
        throw new IllegalArgumentException(
            "Invalid format for Spanner property " + SpannerStorageProvider.COMPRESSION_CONFIG);
      }
    }
    return new DatabaseCompressionConfig(tableCompressionConfigs);
  }

  /**
   * Returns map of column names to CompressionConfig for the specified table, or {@code null} if no
   * compression configuration exists for the table.
   *
   * @param tableName the name of the table for which to retrieve the compression configuration.
   * @return map
   */
  public Map<String, CompressionConfig> get(String tableName) {
    return configs.get(tableName);
  }
}
