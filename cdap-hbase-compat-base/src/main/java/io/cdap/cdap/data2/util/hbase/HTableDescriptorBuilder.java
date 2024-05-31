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

package io.cdap.cdap.data2.util.hbase;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * Builder for creating an HBase {@code HTableDescriptor} object.  This should be used instead of
 * creating an {@code HTableDescriptor} directly in order to avoid API incompatibilities between
 * HBase versions.
 */
public class HTableDescriptorBuilder {

  protected final HTableDescriptor instance;

  HTableDescriptorBuilder(TableName tableName) {
    this.instance = new HTableDescriptor(tableName);
  }

  HTableDescriptorBuilder(HTableDescriptor toCopy) {
    this.instance = new HTableDescriptor(toCopy);
  }

  /* Common methods shared by all HBase versions */
  public HTableDescriptorBuilder removeCoprocessor(String className) {
    instance.removeCoprocessor(className);
    return this;
  }

  public byte[] getValue(byte[] key) {
    return instance.getValue(key);
  }

  public String getValue(String key) {
    return instance.getValue(key);
  }

  /* Methods whose signature changed in HBase 1.0 due to HBASE-10841 */
  public HTableDescriptorBuilder setValue(byte[] key, byte[] value) {
    instance.setValue(key, value);
    return this;
  }

  public HTableDescriptorBuilder setValue(String key, String value) {
    instance.setValue(key, value);
    return this;
  }

  public HTableDescriptorBuilder addFamily(HColumnDescriptor columnDescriptor) {
    instance.addFamily(columnDescriptor);
    return this;
  }

  public HTableDescriptorBuilder addCoprocessor(String className) throws IOException {
    instance.addCoprocessor(className);
    return this;
  }

  public HTableDescriptorBuilder addCoprocessor(String className, Path jarFilePath, int priority,
      Map<String, String> keyValues) throws IOException {
    instance.addCoprocessor(className, jarFilePath, priority, keyValues);
    return this;
  }

  public HTableDescriptor build() {
    return instance;
  }
}
