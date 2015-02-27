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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Identifier for an HBase tables that contains a table prefix, a namespace and a table name
 */
public class TableId {
  private final String tablePrefix;
  private final Id.Namespace namespace;
  private final String tableName;

  private TableId(String tablePrefix, Id.Namespace namespace, String tableName) {
    this.tablePrefix = tablePrefix;
    this.namespace = namespace;
    this.tableName = tableName;
  }

  public String getTablePrefix() {
    return tablePrefix;
  }

  public Id.Namespace getNamespace() {
    return namespace;
  }

  @VisibleForTesting
  public String getHBaseNamespace() {
    // TODO: pass in prefix as first param?
    return HBaseTableUtil.toHBaseNamespace(namespace);
  }

  /**
   * @return Backward compatible, ASCII encoded table name
   */
  public String getTableName() {
    return HBaseTableUtil.getHBaseTableName(getBackwardCompatibleTableName());
  }

  //TODO: rename

  /**
   * @return the CDAP representation of the table name
   */
  public String getCdapTableName() {
    return tableName;
  }

  private String getBackwardCompatibleTableName() {
    // handle table names in default namespace so we do not have to worry about upgrades
    if (Constants.DEFAULT_NAMESPACE_ID.equals(namespace)) {
      // if the table name starts with 'system.', then its a queue or stream table. Do not add namespace to table name
      // e.g. namespace = default, tableName = system.queue.config. Resulting table name = cdap.system.queue.config
      if (tableName.startsWith(String.format("%s.", Constants.SYSTEM_NAMESPACE))) {
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

  public static TableId from(String tablePrefix, String namespace, String tableName) {
    Preconditions.checkArgument(tablePrefix != null, "Table prefix should not be null.");
    Preconditions.checkArgument(tableName != null, "Table name should not be null.");
    // Id.Namespace already checks for non-null namespace
    return new TableId(tablePrefix, Id.Namespace.from(namespace), tableName);
  }

  public static TableId from(String hBaseNamespace, String hTableName) {
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
    return new TableId(prefix, Id.Namespace.from(namespace), hTableName);
  }


  // TODO: rename/cleanup
  public static String fromHBaseNamespace(String hBaseNamespace) {
    // Handle backward compatibility to not add the prefix for default namespace
    if (Constants.DEFAULT_NAMESPACE.equals(hBaseNamespace)) {
      return hBaseNamespace;
    }
    return hBaseNamespace.split("_")[1];
  }

  /**
   * Construct a {@link TableId} from a dataset name
   * TODO: CDAP-1593 - This is bad and should be removed, since it makes assumptions about the dataset name format.
   * Will need a bigger change to have DatasetAdmin classes accept a namespace id in some other form to pass to 
   * #from(tablePrefix, namespace, tableName) as opposed to getting it from the {@link DatasetSpecification#getName}
   *
   * @param name the dataset/table name to construct the {@link TableId} from
   * @return the {@link TableId} object for the specified dataset/table name
   */
  public static TableId from(String name) {
    Preconditions.checkArgument(name != null, "Dataset name should not be null");
    // Dataset/Table name is expected to be in the format <table-prefix>.<namespace>.<name>
    String invalidFormatError = String.format("Invalid format for dataset/table name '%s'. " +
                                                "Expected - <table-prefix>.<namespace>.<dataset/table-name>", name);
    String [] parts = name.split("\\.", 3);
    Preconditions.checkArgument(parts.length == 3, invalidFormatError);
    return new TableId(parts[0], Id.Namespace.from(parts[1]), parts[2]);
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
    return Objects.equal(tablePrefix, that.getTablePrefix()) &&
      Objects.equal(namespace, that.getNamespace()) &&
      Objects.equal(tableName, that.getTableName());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tablePrefix, namespace, tableName);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("tablePrefix", tablePrefix)
      .add("namespace", namespace)
      .add("tableName", tableName)
      .toString();
  }
}
