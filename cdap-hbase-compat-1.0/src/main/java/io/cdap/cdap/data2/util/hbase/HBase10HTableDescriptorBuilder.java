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

package co.cask.cdap.data2.util.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Map;

/**
 * HBase 1.0 specific implementation for {@link HTableDescriptorBuilder}.
 */
public class HBase10HTableDescriptorBuilder extends HTableDescriptorBuilder {
  HBase10HTableDescriptorBuilder(TableName tableName) {
    super(tableName);
  }

  HBase10HTableDescriptorBuilder(HTableDescriptor descriptorToCopy) {
    super(descriptorToCopy);
  }

  /* Methods whose signature changed in HBase 1.0 due to HBASE-10841 */
  @Override
  public HTableDescriptorBuilder setValue(byte[] key, byte[] value) {
    instance.setValue(key, value);
    return this;
  }

  @Override
  public HTableDescriptorBuilder setValue(String key, String value) {
    instance.setValue(key, value);
    return this;
  }

  @Override
  public HTableDescriptorBuilder addFamily(HColumnDescriptor columnDescriptor) {
    instance.addFamily(columnDescriptor);
    return this;
  }

  @Override
  public HTableDescriptorBuilder addCoprocessor(String className) throws IOException {
    instance.addCoprocessor(className);
    return this;
  }

  @Override
  public HTableDescriptorBuilder addCoprocessor(String className, Path jarFilePath, int priority,
                                                Map<String, String> keyValues) throws IOException {
    instance.addCoprocessor(className, jarFilePath, priority, keyValues);
    return this;
  }
}
