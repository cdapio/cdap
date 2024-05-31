/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.cdap.cdap.data2.util.TableId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * Common utility methods for dealing with HBase table name conversions.
 */
public final class HTableNameConverter {

  private HTableNameConverter() {

  }

  /**
   * Encode a HBase entity name to ASCII encoding using {@link URLEncoder}.
   *
   * @param entityName entity string to be encoded
   * @return encoded string
   */
  public static String encodeHBaseEntity(String entityName) {
    try {
      return URLEncoder.encode(entityName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      throw new RuntimeException(e);
    }
  }

  private static String getBackwardCompatibleTableName(String tablePrefix, TableId tableId) {
    String tableName = tableId.getTableName();
    // handle table names in default namespace so we do not have to worry about upgrades
    // cdap default namespace always maps to hbase default namespace and vice versa
    if (NamespaceId.DEFAULT.getNamespace().equals(tableId.getNamespace())) {
      // if the table name starts with 'system.', then its a queue or stream table. Do not add namespace to table name
      // e.g. namespace = default, tableName = system.queue.config. Resulting table name = cdap.system.queue.config
      // also no need to prepend the table name if it already starts with 'user'.
      // TODO: the 'user' should be prepended by the HBaseTableAdmin.
      if (tableName.startsWith(String.format("%s.", NamespaceId.SYSTEM.getNamespace()))) {
        return Joiner.on(".").join(tablePrefix, tableName);
      }
      // if the table name does not start with 'system.', then its a user dataset. Add 'user' to the table name to
      // maintain backward compatibility. Also, do not add namespace to the table name
      // e.g. namespace = default, tableName = purchases. Resulting table name = cdap.user.purchases
      return Joiner.on(".").join(tablePrefix, "user", tableName);
    }
    // if the namespace is not default, do not need to change anything
    return tableName;
  }

  /**
   * @return Backward compatible, ASCII encoded table name
   */
  public static String toHBaseTableName(String tablePrefix, TableId tableId) {
    Preconditions.checkArgument(tablePrefix != null, "Table prefix should not be null.");
    return encodeHBaseEntity(getBackwardCompatibleTableName(tablePrefix, tableId));
  }

  /**
   * Gets the system configuration table prefix.
   *
   * @param prefix Prefix string
   * @return System configuration table prefix (full table name minus the table qualifier). Example
   *     input: "cdap.table.name"     -->  output: "cdap_system."   (input table is in default
   *     namespace) Example input: "cdap_ns:table.name"  -->  output: "cdap_system:"   (input table
   *     is in a custom namespace)
   */
  public static String getSysConfigTablePrefix(String prefix) {
    return prefix + "_" + NamespaceId.SYSTEM.getNamespace() + ":";
  }

  /**
   * @return the hbase namespace representing the CDAP system namespace.
   */
  public static String getSystemNamespace(String prefix) {
    return prefix + "_" + NamespaceId.SYSTEM.getNamespace();
  }

  /**
   * Returns {@link TableId} for the table represented by the given {@link HTableDescriptor}.
   */
  public static TableId from(HTableDescriptor htd) {
    TableName tableName = htd.getTableName();
    return fromHBaseTableName(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
  }

  /**
   * Construct and return the HBase tableName from {@link TableId} and tablePrefix.
   */
  public static TableName toTableName(String tablePrefix, TableId tableId) {
    return TableName.valueOf(tableId.getNamespace(), toHBaseTableName(tablePrefix, tableId));
  }

  private static TableId fromHBaseTableName(String namespace, String qualifier) {
    Preconditions.checkArgument(namespace != null, "Table namespace should not be null.");
    Preconditions.checkArgument(qualifier != null, "Table qualifier should not be null.");

    // Handle backward compatibility to not add the prefix for default namespace
    if (NamespaceId.DEFAULT.getEntityName().equals(namespace)) {
      // in Default namespace, qualifier is something like 'cdap.foo.table'
      @SuppressWarnings("ConstantConditions")
      String[] parts = qualifier.split("\\.", 2);
      Preconditions.checkArgument(parts.length == 2,
          String.format("expected table name to contain '.': %s", qualifier));
      qualifier = parts[1];

      // strip 'user.' from the beginning of table name since we prepend it in getBackwardCompatibleTableName
      parts = qualifier.split("\\.", 2);
      if (parts.length == 2 && "user".equals(parts[0])) {
        qualifier = parts[1];
      }
      return TableId.from(namespace, qualifier);
    }
    return TableId.from(namespace, qualifier);
  }
}
