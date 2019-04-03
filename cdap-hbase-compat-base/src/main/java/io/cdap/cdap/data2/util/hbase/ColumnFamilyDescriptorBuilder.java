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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.spi.hbase.ColumnFamilyDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for {@link ColumnFamilyDescriptor}.
 */
public class ColumnFamilyDescriptorBuilder {
  private final String name;
  private final Map<String, String> properties;

  private int maxVersions;
  private ColumnFamilyDescriptor.CompressionType compressionType;
  private ColumnFamilyDescriptor.BloomType bloomType;

  public ColumnFamilyDescriptorBuilder(String name) {
    this.name = name;
    this.properties = new HashMap<>();

    // Default maxVersions is 1
    this.maxVersions = 1;
    // Default compression type
    this.compressionType = ColumnFamilyDescriptor.CompressionType.SNAPPY;
    // Default bloom type
    this.bloomType = ColumnFamilyDescriptor.BloomType.ROW;
  }

  public ColumnFamilyDescriptorBuilder setMaxVersions(int n) {
    this.maxVersions = n;
    return this;
  }

  public ColumnFamilyDescriptorBuilder setCompressionType(ColumnFamilyDescriptor.CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  public ColumnFamilyDescriptorBuilder setBloomType(ColumnFamilyDescriptor.BloomType bloomType) {
    this.bloomType = bloomType;
    return this;
  }

  public ColumnFamilyDescriptorBuilder addProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public ColumnFamilyDescriptor build() {
    return new ColumnFamilyDescriptor(name, maxVersions, compressionType, bloomType, properties);
  }
}
