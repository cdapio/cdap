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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.spi.data.compression.Compressor;
import java.util.Map;

/**
 * Factory class for creating and retrieving {@link Compressor} instances based on
 * {@link CompressorType}.
 */
public class CompressorFactory {

  private static final Map<CompressorType, Compressor> compressors = ImmutableMap.<CompressorType, Compressor>builder()
      .put(CompressorType.SNAPPY, new SnappyCompressor()).build();

  /**
   * Method to retrieve the Compressor instance for a given CompressorType.
   */
  public static Compressor getCompressor(CompressorType type) {
    Compressor compressor = compressors.get(type);
    if (compressor == null) {
      throw new IllegalArgumentException("Unsupported compression type: " + type);
    }
    return compressor;
  }
}
