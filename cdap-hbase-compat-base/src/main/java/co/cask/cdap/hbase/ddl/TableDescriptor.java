/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HTableNameConverter;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Describes an HBase table.
 */
public final class TableDescriptor {
  public static final String CDAP_VERSION = "cdap.version";
  private static final Logger LOG = LoggerFactory.getLogger(TableDescriptor.class);

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

    this.properties = ImmutableMap.copyOf(properties);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public Map<String, ColumnFamilyDescriptor> getFamilies() {
    return ImmutableMap.copyOf(families);
  }

  public Map<String, CoprocessorDescriptor> getCoprocessors() {
    return ImmutableMap.copyOf(coprocessors);
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public ProjectInfo.Version getCDAPVersion() {
    return new ProjectInfo.Version(properties.get(CDAP_VERSION));
  }

  public String getTablePrefix() {
    return properties.get(Constants.Dataset.TABLE_PREFIX);
  }

  /**
   * A Builder to construct TableDescriptor.
   */
  public static class Builder {
    private Set<ColumnFamilyDescriptor> families = new HashSet<>();
    private Set<CoprocessorDescriptor> coprocessors = new HashSet<>();
    private final Map<String, String> properties;

    private final TableId tableId;
    private final String tablePrefix;
    private final HTableNameConverter nameConverter = new HTableNameConverter();

    public Builder(CConfiguration cConf, TableId tableId) {
      this.tableId = tableId;
      this.properties = new HashMap<>();
      properties.put(CDAP_VERSION, ProjectInfo.getVersion().toString());
      this.tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
      properties.put(Constants.Dataset.TABLE_PREFIX, tablePrefix);
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
      TableName tableName = nameConverter.toTableName(tablePrefix, tableId);
      return new TableDescriptor(tableName.getNamespaceAsString(), tableName.getQualifierAsString(), families,
                                 coprocessors, properties);
    }
  }
}
