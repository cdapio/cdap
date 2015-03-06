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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.util.TableId;
import org.apache.hadoop.hbase.TableName;

/**
 * Utility methods for dealing with HBase table name conversions in HBase 0.96.
 */
public class HTable96NameConverter extends HTableNameConverter {
  @Override
  public String getSysConfigTablePrefix(String hTableName) {
    return getHbaseNamespacePrefix(TableName.valueOf(hTableName)) + "_" + Constants.SYSTEM_NAMESPACE + ":";
  }

  public static TableName toTableName(CConfiguration cConf, TableId tableId) {
    String tablePrefix = cConf.get(Constants.Dataset.TABLE_PREFIX);
    return toTableName(tablePrefix, tableId);
  }

  public static TableName toTableName(String tablePrefix, TableId tableId) {
    return TableName.valueOf(toHBaseNamespace(tablePrefix, tableId.getNamespace()),
                             getHBaseTableName(tablePrefix, tableId));
  }

  public static TableId fromTableName(TableName tableName) {
    return prefixedTableIdFromTableName(tableName).getTableId();
  }

  public static String getHbaseNamespacePrefix(TableName tableName) {
    return prefixedTableIdFromTableName(tableName).getTablePrefix();
  }

  private static PrefixedTableId prefixedTableIdFromTableName(TableName tableName) {
    return HTableNameConverter.from(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
  }
}
