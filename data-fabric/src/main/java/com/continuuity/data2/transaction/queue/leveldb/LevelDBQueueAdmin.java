package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * admin for queues in leveldb.
 */
@Singleton
public class LevelDBQueueAdmin implements QueueAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(LevelDBQueueAdmin.class);

  private final String tableName;
  private final LevelDBOcTableService service;
  private final String namespace;

  @Inject
  public LevelDBQueueAdmin(DataSetAccessor dataSetAccessor, LevelDBOcTableService service) {
    this(dataSetAccessor, service, "queue");
  }

  protected LevelDBQueueAdmin(DataSetAccessor dataSetAccessor, LevelDBOcTableService service, String namespace) {
    this.service = service;
    this.namespace = namespace;
    // todo: we have to do that because queues do not follow dataset semantic fully (yet)
    this.tableName = dataSetAccessor.namespace(namespace, DataSetAccessor.Namespace.SYSTEM);
  }

  @Override
  public boolean exists(@SuppressWarnings("unused") String name) throws Exception {
    try {
      service.getTable(tableName);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void create(@SuppressWarnings("unused") String name) throws Exception {
    service.ensureTableExists(tableName);
  }

  @Override
  public void create(String name, @SuppressWarnings("unused") Properties props) throws Exception {
    create(name);
  }

  @Override
  public void truncate(@SuppressWarnings("unused") String name) throws Exception {
    // todo: right now, we can only truncate all queues at once.
    drop(tableName);
    create(tableName);
  }

  @Override
  public void drop(@SuppressWarnings("unused") String name) throws Exception {
    // No-op, as all queue entries are in one table.
    LOG.warn("Drop({}) on HBase {} table has no effect.", name, namespace);
  }

  @Override
  public void dropAll() throws Exception {
    // hack: we know that all queues stored in one table
    service.dropTable(tableName);
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

  public String getTableName() {
    return tableName;
  }
}
