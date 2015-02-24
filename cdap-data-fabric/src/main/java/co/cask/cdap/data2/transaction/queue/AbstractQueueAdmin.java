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

package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.proto.Id;

/**
 * Common implementation of table-based QueueAdmin
 */
public abstract class AbstractQueueAdmin implements QueueAdmin {
  private final String unqualifiedTableNamePrefix;
  private final String unqualifiedConfigTableNamePrefix;
  protected final DefaultDatasetNamespace namespace;

  public AbstractQueueAdmin(CConfiguration conf, QueueConstants.QueueType type) {
    // system scoped
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    this.unqualifiedTableNamePrefix = Constants.SYSTEM_NAMESPACE + "." + type.toString();
    this.unqualifiedConfigTableNamePrefix = Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QUEUE_CONFIG_TABLE_NAME;
    this.namespace = new DefaultDatasetNamespace(conf);
  }

  /**
   * @param queueTableName actual queue table name
   * @return namespace id that this queue belongs to
   */
  public static String getNamespaceId(String queueTableName) {
    // last three parts are namespaceId, appName and flow
    String[] parts = queueTableName.split("\\.");
    String namespaceId;
    if (parts.length == 6) {
      // cdap.<namespace>.system.queue.<app>.<flow>
      namespaceId = parts[1];
    } else {
      throw new IllegalArgumentException(String.format("Unexpected format for queue table name. " +
                                                         "Expected 'cdap.<namespace>.system.queue.<app>.<flow>'" +
                                                         "Received '%s'",
                                                       queueTableName));
    }
    return namespaceId;
  }

  /**
   * @param queueTableName actual queue table name
   * @return app name this queue belongs to
   */
  public static String getApplicationName(String queueTableName) {
    // last three parts are namespaceId (optional - in which case it will be the default namespace), appName and flow
    String[] parts = queueTableName.split("\\.");
    return parts[parts.length - 2];
  }

  /**
   * @param queueTableName actual queue table name
   * @return flow name this queue belongs to
   */
  public static String getFlowName(String queueTableName) {
    // last three parts are namespaceId (optional - in which case it will be the default namespace), appName and flow
    String[] parts = queueTableName.split("\\.");
    return parts[parts.length - 1];
  }


  /**
   * This determines the actual table name from the table name prefix and the name of the queue.
   * @param queueName The name of the queue.
   * @return the full name of the table that holds this queue.
   */
  public String getActualTableName(QueueName queueName) {
    if (queueName.isQueue()) {
      // <root namespace>.<queue namespace>.system.queue.<app>.<flow>
      return getTableNameForFlow(queueName.getFirstComponent(),
                                 queueName.getSecondComponent(),
                                 queueName.getThirdComponent());
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
  }

  protected String getTableNameForFlow(String namespaceId, String app, String flow) {
    String tablePrefix = getTableNamePrefix(namespaceId);
    String tableName = tablePrefix + "." + app + "." + flow;
    return HBaseTableUtil.getHBaseTableName(tableName);
  }

  public String getTableNamePrefix(String namespaceId) {
    // returns String with format:  '<root namespace>.<namespaceId>.system.(stream|queue)'
    String tablePrefix = namespace.namespace(Id.DatasetInstance.from(namespaceId,
                                                                     unqualifiedTableNamePrefix)).getId();
    return HBaseTableUtil.getHBaseTableName(tablePrefix);
  }

  public String getConfigTableName(QueueName queueName) {
    return getConfigTableName(queueName.getFirstComponent());
  }

  public String getConfigTableName(String namespaceId) {
    // returns String with format:  '<root namespace>.<namespaceId>.system.queue.config'
    String tablePrefix = namespace.namespace(Id.DatasetInstance.from(namespaceId,
                                                                     unqualifiedConfigTableNamePrefix)).getId();
    return HBaseTableUtil.getHBaseTableName(tablePrefix);
  }
}
