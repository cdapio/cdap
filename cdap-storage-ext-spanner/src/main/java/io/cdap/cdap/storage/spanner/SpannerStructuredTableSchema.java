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

package io.cdap.cdap.storage.spanner;

import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.storage.spanner.compression.CompressionConfig;
import io.cdap.cdap.storage.spanner.compression.CompressorType;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Wrapper over {@link StructuredTableSchema} that stores spanner specific config related to
 * compression along with the table schema.
 */
public class SpannerStructuredTableSchema extends StructuredTableSchema {

  /**
   * Map of column name and compression config.
   */
  private final Map<String, CompressionConfig> compressionConfigs;

  /**
   * Constructor for {@link SpannerStructuredTableSchema}.
   */
  SpannerStructuredTableSchema(StructuredTableId tableId,
      List<FieldType> fields,
      List<String> primaryKeys,
      Collection<String> indexes,
      Map<String, CompressionConfig> compressionConfigs) {
    super(tableId, fields, primaryKeys, indexes);
    this.compressionConfigs = compressionConfigs == null ? Collections.emptyMap()
        : Collections.unmodifiableMap(compressionConfigs);
  }

  /**
   * Retrieves compressor type for field.
   *
   * @param field column name for which compressor needs to be determined.
   * @return compressor type set in cConf
   */
  CompressorType getCompressorType(String field) {
    CompressionConfig config = compressionConfigs.get(field);
    return config == null ? null : config.getCompressorType();
  }
}
