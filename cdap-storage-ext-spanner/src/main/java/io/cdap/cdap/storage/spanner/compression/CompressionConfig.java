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

/**
 * Configuration class for compression settings.
 */
public class CompressionConfig {

  /**
   * Suffix for compressed columns.
   */
  public static final String COMPRESSED_COLUMN_SUFFIX = "_compressor_type";

  private final CompressorType compressorType;

  /**
   * Constructor for {@link CompressionConfig}.
   *
   * @param compressorType the {@link CompressorType} to be used.
   */
  public CompressionConfig(CompressorType compressorType) {
    this.compressorType = compressorType;
  }

  /**
   * Returns the {@link CompressorType}.
   */
  public CompressorType getCompressorType() {
    return compressorType;
  }
}
