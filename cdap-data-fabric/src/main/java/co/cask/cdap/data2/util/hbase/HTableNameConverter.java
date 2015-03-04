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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Common utility methods for dealing with HBase table name conversions.
 */
public abstract class HTableNameConverter {
  protected static final String HBASE_NAMESPACE_PREFIX = "cdap_";

  public static String getHBaseTableName(String tableName) {
    return encodeTableName(tableName);
  }

  public static String encodeTableName(String tableName) {
    try {
      return URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the system configuration table prefix
   * @param hTableName Full HBase table name.
   * @return System configuration table prefix (full table name minus the table qualifier).
   * Example input: "cdap_ns.table.name"  -->  output: "cdap_system."   (hbase 94)
   * Example input: "cdap.table.name"     -->  output: "cdap_system."   (hbase 94. input table is in default namespace)
   * Example input: "cdap_ns:table.name"  -->  output: "cdap_system:"   (hbase 96, 98)
   */
  public abstract String getSysConfigTablePrefix(String hTableName);

  public abstract TableId fromTableName(String hTableName);

  @VisibleForTesting
  public static String toHBaseNamespace(Id.Namespace namespace) {
    // Handle backward compatibility to not add the prefix for default namespace
    // TODO: CDAP-1601 - Conditional should be removed when we have a way to upgrade user datasets
    return HTableNameConverter.getHBaseTableName(Constants.DEFAULT_NAMESPACE_ID.equals(namespace) ? namespace.getId() :
                                                   HBASE_NAMESPACE_PREFIX + namespace.getId());
  }

  /**
   * @return Backward compatible, ASCII encoded table name
   */
  public static String getHBaseTableName(TableId tableId) {
    return HTableNameConverter.getHBaseTableName(tableId.getBackwardCompatibleTableName());
  }

  protected static TableId from(String hBaseNamespace, String hTableName) {
    Preconditions.checkArgument(hBaseNamespace != null, "Table namespace should not be null.");
    Preconditions.checkArgument(hTableName != null, "Table name should not be null.");

    String namespace;
    String prefix;

    // Handle backward compatibility to not add the prefix for default namespace
    if (Constants.DEFAULT_NAMESPACE.equals(hBaseNamespace)) {
      namespace = hBaseNamespace;
      // in Default namespace, hTableName is something like 'cdap.foo.table'
      String[] parts = hTableName.split("\\.", 2);
      Preconditions.checkArgument(parts.length == 2,
                                  String.format("expected table name to contain '.': %s", hTableName));
      prefix = parts[0];
      hTableName = parts[1];
      return TableId.from(prefix, namespace, hTableName);
    }


    String[] parts = hBaseNamespace.split("_");
    Preconditions.checkArgument(parts.length == 2,
                                String.format("expected hbase namespace to have a '_': %s", hBaseNamespace));
    prefix = parts[0];
    namespace = parts[1];

    // Id.Namespace already checks for non-null namespace
    return TableId.from(prefix, namespace, hTableName);
  }
}
