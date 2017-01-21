/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spi.hbase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Descripbes HBase table column family.
 */
public final class ColumnFamilyDescriptor {

  private final String name;
  private final int maxVersions;
  private final CompressionType compressionType;
  private final BloomType bloomType;
  private final Map<String, String> properties;

  /**
   * Represents the compression types supported for HBase tables.
   */
  public enum CompressionType {
    LZO, SNAPPY, GZIP, NONE
  }

  /**
   * Represents the bloom filter types supported for HBase tables.
   */
  public enum BloomType {
    ROW, ROWCOL, NONE
  }

  public ColumnFamilyDescriptor(String name, int maxVersions, CompressionType compressionType,
                                BloomType bloomType, Map<String, String> properties) {
    this.name = name;
    this.maxVersions = maxVersions;
    this.compressionType = compressionType;
    this.bloomType = bloomType;
    this.properties = properties == null ? Collections.<String, String>emptyMap()
      : Collections.unmodifiableMap(properties);
  }

  public String getName() {
    return name;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public BloomType getBloomType() {
    return bloomType;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Builder for {@link ColumnFamilyDescriptor}.
   */
  public static class Builder {
    private final String name;
    private final Map<String, String> properties;

    private int maxVersions;
    private CompressionType compressionType;
    private BloomType bloomType;

    public Builder(String name) {
      this.name = name;
      this.properties = new HashMap<>();

      // Default maxVersions is 1
      this.maxVersions = 1;
      // Default compression type
      this.compressionType = CompressionType.SNAPPY;
      // Default bloom type
      this.bloomType = BloomType.ROW;
    }

    public Builder setMaxVersions(int n) {
      this.maxVersions = n;
      return this;
    }

    public Builder setCompressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return this;
    }

    public Builder setBloomType(BloomType bloomType) {
      this.bloomType = bloomType;
      return this;
    }

    public Builder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public ColumnFamilyDescriptor build() {
      return new ColumnFamilyDescriptor(name, maxVersions, compressionType, bloomType, properties);
    }
  }
}
