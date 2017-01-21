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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Describes an HBase table.
 */
public final class TableDescriptor {

  private final String namespace;
  private final String name;
  private final Map<String, ColumnFamilyDescriptor> families;
  private final Map<String, CoprocessorDescriptor> coprocessors;
  private final Map<String, String> properties;

  public TableDescriptor(String namespace, String name, Set<ColumnFamilyDescriptor> families,
                         Set<CoprocessorDescriptor> coprocessors, Map<String, String> properties) {
    this.namespace = namespace;
    this.name = name;

    this.families = new HashMap<>();
    for (ColumnFamilyDescriptor family : families) {
      this.families.put(family.getName(), family);
    }

    this.coprocessors = new HashMap<>();
    for (CoprocessorDescriptor coprocessor : coprocessors) {
      this.coprocessors.put(coprocessor.getClassName(), coprocessor);
    }

    this.properties = properties == null ? Collections.<String, String>emptyMap()
      : Collections.unmodifiableMap(properties);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public Map<String, ColumnFamilyDescriptor> getFamilies() {
    return Collections.unmodifiableMap(families);
  }

  public Map<String, CoprocessorDescriptor> getCoprocessors() {
    return Collections.unmodifiableMap(coprocessors);
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * A Builder to construct TableDescriptor.
   */
  public static class Builder {
    private final String namespace;
    private final String tableName;

    private Set<ColumnFamilyDescriptor> families = new HashSet<>();
    private Set<CoprocessorDescriptor> coprocessors = new HashSet<>();
    private final Map<String, String> properties;

    // TODO should we call tableName as qualifier
    // TODO should caller always set prefix and cdap version in properties
    public Builder(String namespace, String tableName) {
      this.namespace = namespace;
      this.tableName = tableName;
      this.properties = new HashMap<>();
    }

    public Builder addCoprocessor(CoprocessorDescriptor coprocessor) {
      this.coprocessors.add(coprocessor);
      return this;
    }

    public Builder addColumnFamily(ColumnFamilyDescriptor family) {
      this.families.add(family);
      return this;
    }

    public Builder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    public TableDescriptor build() {
      return new TableDescriptor(namespace, tableName, families, coprocessors, properties);
    }
  }
}
