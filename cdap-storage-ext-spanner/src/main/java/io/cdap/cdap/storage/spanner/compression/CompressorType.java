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
 * Identifies the compressor type.
 */
public enum CompressorType {
  SNAPPY;

  /**
   * Returns the {@link CompressorType} for the given string value.
   *
   * @param value the string value representing the compressor type
   * @return the corresponding {@link CompressorType}, or {@code null} if the value is invalid
   */
  public static CompressorType fromString(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    return valueOf(value.toUpperCase());
  }
}
