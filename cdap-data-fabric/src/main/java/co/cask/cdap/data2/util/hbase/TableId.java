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

import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.util.Iterator;

/**
 * Identifier for an HBase tables that contains namespace and a table name
 */
public class TableId {
  private final Id.Namespace namespace;
  private final String tableName;

  private TableId(Id.Namespace namespace, String tableName) {
    this.namespace = namespace;
    this.tableName = tableName;
  }

  public Id.Namespace getNamespace() {
    return namespace;
  }

  public String getTableName() {
    return tableName;
  }

  public static TableId from(String namespace, String tableName) {
    Preconditions.checkArgument(tableName != null, "Table name should not be null.");
    // Id.Namespace already checks for non-null namespace
    return new TableId(Id.Namespace.from(namespace), tableName);
  }

  public static TableId from(String datasetName) {
    Preconditions.checkArgument(datasetName != null, "Dataset name should not be null");
    // Dataset name is of the format cdap.<namespace>.<dataset-name>
    String invalidFormatError = String.format("Incvalid format for dataset name '%s'. E" +
                                                "xpected - cdap.<namespace>.<dataset-name>", datasetName);
    String [] parts = datasetName.split("\\.", 3);
    Preconditions.checkArgument(parts.length == 3, invalidFormatError);
    return new TableId(Id.Namespace.from(parts[1]), parts[2]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableId)) {
      return false;
    }

    TableId that = (TableId) o;
    return Objects.equal(namespace, that.getNamespace()) &&
      Objects.equal(tableName, that.getTableName());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(namespace, tableName);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("namespace", namespace)
      .add("tableName", tableName)
      .toString();
  }
}
