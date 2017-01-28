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
import co.cask.cdap.spi.hbase.CoprocessorDescriptor;
import co.cask.cdap.spi.hbase.TableDescriptor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Builder for {@link TableDescriptor}.
 */
public class TableDescriptorBuilder {
  private final String namespace;
  private final String tableName;

  private Set<ColumnFamilyDescriptor> families = new HashSet<>();
  private Set<CoprocessorDescriptor> coprocessors = new HashSet<>();
  private final Map<String, String> properties;

  public TableDescriptorBuilder(String namespace, String tableName) {
    this.namespace = namespace;
    this.tableName = tableName;
    this.properties = new HashMap<>();
  }

  public TableDescriptorBuilder(TableDescriptor descriptor) {
    this.namespace = descriptor.getNamespace();
    this.tableName = descriptor.getName();
    this.properties = descriptor.getProperties();
    this.families.addAll(descriptor.getFamilies().values());
    this.coprocessors.addAll(descriptor.getCoprocessors().values());
  }

  public TableDescriptorBuilder addCoprocessor(CoprocessorDescriptor coprocessor) {
    this.coprocessors.add(coprocessor);
    return this;
  }

  public TableDescriptorBuilder addColumnFamily(ColumnFamilyDescriptor family) {
    this.families.add(family);
    return this;
  }

  public TableDescriptorBuilder addProperty(String key, String value) {
    this.properties.put(key, value);
    return this;
  }

  public TableDescriptor build() {
    return new TableDescriptor(namespace, tableName, families, coprocessors, properties);
  }
}


