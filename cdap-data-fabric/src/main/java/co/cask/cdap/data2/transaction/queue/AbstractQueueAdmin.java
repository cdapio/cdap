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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.util.TableId;

/**
 * Common implementation of table-based QueueAdmin
 */
public abstract class AbstractQueueAdmin implements QueueAdmin {
  protected final QueueConstants.QueueType type;
  protected final String unqualifiedTableNamePrefix;
  // system.queue.config'
  private static final String CONFIG_TABLE_NAME =
    Constants.SYSTEM_NAMESPACE + "." + QueueConstants.QUEUE_CONFIG_TABLE_NAME;

  public AbstractQueueAdmin(QueueConstants.QueueType type) {
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    // system scoped
    this.unqualifiedTableNamePrefix = Constants.SYSTEM_NAMESPACE + "." + type.toString();
    this.type = type;
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

  public static  TableId getConfigTableId(QueueName queueName) {
    return getConfigTableId(queueName.getFirstComponent());
  }

  public static TableId getConfigTableId(String namespace) {
    return TableId.from(namespace, CONFIG_TABLE_NAME);
  }

  /**
   * This determines the actual TableId from the table name prefix and the name of the queue.
   * @param queueName The name of the queue.
   * @return the full name of the table that holds this queue.
   */
  public TableId getDataTableId(QueueName queueName) {
    if (queueName.isQueue()) {
      return getDataTableId(queueName.getFirstComponent(),
                            queueName.getSecondComponent(),
                            queueName.getThirdComponent());
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
  }

  public TableId getDataTableId(String namespaceId, String app, String flow) {
    // tableName = system.queue.<app>.<flow>
    String tableName = unqualifiedTableNamePrefix + "." + app + "." + flow;
    return TableId.from(namespaceId, tableName);
  }
}
