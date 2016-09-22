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

import co.cask.cdap.proto.id.FlowId;
import co.cask.cdap.proto.id.NamespaceId;

/**
 * Common implementation of table-based QueueAdmin
 */
public abstract class AbstractQueueAdmin implements QueueAdmin {
  protected final QueueConstants.QueueType type;
  protected final String unqualifiedTableNamePrefix;

  public AbstractQueueAdmin(QueueConstants.QueueType type) {
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    // system scoped
    this.unqualifiedTableNamePrefix = NamespaceId.SYSTEM.getEntityName() + "." + type.toString();
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

  public String getDataTableName(FlowId flowId) {
    // tableName = system.queue.<app>.<flow>
    return unqualifiedTableNamePrefix + "." + flowId.getApplication() + "." + flowId.getEntityName();
  }
}
