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

package co.cask.cdap.hbase.ddl;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Descripbes HBase table column family.
 */
public final class ColumnFamilyDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnFamilyDescriptor.class);
  public static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.SNAPPY;
  public static final String CFG_HBASE_TABLE_COMPRESSION = "hbase.table.compression.default";

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
    this.properties = ImmutableMap.copyOf(properties);
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
    private int maxVersions;
    private CompressionType compressionType;
    private BloomType bloomType;
    private final Map<String, String> properties;

    public Builder(Configuration hConf, String name) {
      this.name = name;
      String compression = hConf.get(CFG_HBASE_TABLE_COMPRESSION, DEFAULT_COMPRESSION_TYPE.name());
      this.compressionType = CompressionType.valueOf(compression);
      this.bloomType = BloomType.ROW;
      this.maxVersions = 1;
      this.properties = new HashMap<>();
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
