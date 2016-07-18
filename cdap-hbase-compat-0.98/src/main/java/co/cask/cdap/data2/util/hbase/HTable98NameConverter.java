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

import co.cask.cdap.data2.util.TableId;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/**
 * Utility methods for dealing with HBase table name conversions in HBase 0.98.
 */
public class HTable98NameConverter extends HTableNameConverter {

  @Override
  public TableId from(HTableDescriptor htd) {
    TableName tableName = htd.getTableName();
    return fromHBaseTableName(tableName.getNamespaceAsString(), tableName.getQualifierAsString());
  }

  @Override
  public TableName toTableName(String tablePrefix, TableId tableId) {
    return TableName.valueOf(tableId.getNamespace(), toHBaseTableName(tablePrefix, tableId));
  }
}
