/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBOrderedTableService;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConstants;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import static co.cask.cdap.data2.transaction.queue.QueueConstants.QueueType.QUEUE;

/**
 * admin for queues in leveldb.
 */
@Singleton
public class LevelDBQueueAdmin implements QueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBQueueAdmin.class);

  private final String tableNamePrefix;
  private final LevelDBOrderedTableService service;

  @Inject
  public LevelDBQueueAdmin(CConfiguration conf, LevelDBOrderedTableService service) {
    this(conf, service, QUEUE);
  }

  protected LevelDBQueueAdmin(CConfiguration conf, LevelDBOrderedTableService service,
                              QueueConstants.QueueType type) {
    this.service = service;
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    String unqualifiedTableNamePrefix =
      type == QUEUE ? QueueConstants.QUEUE_TABLE_PREFIX : QueueConstants.STREAM_TABLE_PREFIX;
    this.tableNamePrefix = new DefaultDatasetNamespace(conf, Namespace.SYSTEM).namespace(unqualifiedTableNamePrefix);
  }

  /**
   * This determines the actual table name from the table name prefix and the name of the queue.
   * @param queueName The name of the queue.
   * @return the full name of the table that holds this queue.
   */
  public String getActualTableName(QueueName queueName) {
    if (queueName.isQueue()) {
      // <cdap namespace>.system.queue.<namespace>.<app>.<flow>
      return getTableNameForFlow(queueName.getFirstComponent(),
                                 queueName.getSecondComponent(),
                                 queueName.getThirdComponent());
    } else {
      throw new IllegalArgumentException("'" + queueName + "' is not a valid name for a queue.");
    }
  }

  private String getTableNameForFlow(String namespaceId, String app, String flow) {
    StringBuilder tableName = new StringBuilder(tableNamePrefix).append(".");
    if (!Constants.DEFAULT_NAMESPACE.equals(namespaceId)) {
      tableName.append(namespaceId).append(".");
    }
    return tableName.append(app).append(".").append(flow).toString();
  }

  /**
   * This determines whether dropping a queue is supported (by dropping the queue's table).
   */
  public boolean doDropTable(@SuppressWarnings("unused") QueueName queueName) {
    // no-op because this would drop all tables for the flow
    // todo: introduce a method dropAllFor(flow) or similar
    return false;
  }

  /**
   * This determines whether truncating a queue is supported (by truncating the queue's table).
   */
  public boolean doTruncateTable(@SuppressWarnings("unused") QueueName queueName) {
    // yes, this will truncate all queues of the flow. But it rarely makes sense to clear a single queue.
    // todo: introduce a method truncateAllFor(flow) or similar, and set this to false
    return true;
  }

  @Override
  public boolean exists(@SuppressWarnings("unused") String name) {
    try {
      String actualTableName = getActualTableName(QueueName.from(URI.create(name)));
      service.getTable(actualTableName);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void create(String name) throws Exception {
    create(QueueName.from(URI.create(name)));
  }

  public void create(QueueName queueName) throws Exception {
    String actualTableName = getActualTableName(queueName);
    service.ensureTableExists(actualTableName);
  }

  @Override
  public void create(String name, @SuppressWarnings("unused") Properties props) throws Exception {
    create(name);
  }

  @Override
  public void truncate(String name) throws Exception {
    QueueName queueName = QueueName.from(URI.create(name));
    // all queues for one flow are stored in same table, and we would clear all of them. this makes it optional.
    if (doTruncateTable(queueName)) {
      String actualTableName = getActualTableName(queueName);
      service.dropTable(actualTableName);
      service.ensureTableExists(actualTableName);
    } else {
      LOG.warn("truncate({}) on LevelDB queue table has no effect.", name);
    }
  }

  @Override
  public void clearAllForFlow(String namespaceId, String app, String flow) throws Exception {
    String tableName = getTableNameForFlow(namespaceId, app, flow);
    service.dropTable(tableName);
    service.ensureTableExists(tableName);
  }

  @Override
  public void dropAllForFlow(String namespaceId, String app, String flow) throws Exception {
    String tableName = getTableNameForFlow(namespaceId, app, flow);
    service.dropTable(tableName);
  }

  @Override
  public void drop(String name) throws Exception {
    QueueName queueName = QueueName.from(URI.create(name));
    // all queues for one flow are stored in same table, and we would drop all of them. this makes it optional.
    if (doDropTable(queueName)) {
      String actualTableName = getActualTableName(queueName);
      service.dropTable(actualTableName);
    } else {
      LOG.warn("drop({}) on LevelDB queue table has no effect.", name);
    }
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
    // No-op
  }

  @Override
  public void dropAll() throws Exception {
    for (String tableName : service.list()) {
      if (tableName.startsWith(tableNamePrefix)) {
        service.dropTable(tableName);
      }
    }
  }

  @Override
  public void configureInstances(QueueName queueName, long groupId, int instances) {
    // No-op
    // Potentially refactor QueueClientFactory to have better way to handle instances and group info.
  }

  @Override
  public void configureGroups(QueueName queueName, Map<Long, Integer> groupInfo) {
    // No-op
    // Potentially refactor QueueClientFactory to have better way to handle instances and group info.
  }

  @Override
  public void upgrade() throws Exception {
    // No-op
  }

  protected String getTableNamePrefix() {
    return tableNamePrefix;
  }
}
