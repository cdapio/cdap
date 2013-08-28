package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 *
 */
@Singleton
public class LevelDBQueueAdmin implements QueueAdmin {
  public static final String QUEUE_TABLE_NAME = "__queues";

  private final LevelDBOcTableService service;

  @Inject
  public LevelDBQueueAdmin(LevelDBOcTableService service) {
    this.service = service;
  }

  @Override
  public boolean exists(String name) throws Exception {
    throw new UnsupportedOperationException("Exist check is not supported for LevelDB. " +
                                              "You should call create() to ensure table exists.");
  }

  @Override
  public void create(String name) throws Exception {
    service.ensureTableExists(name);
  }

  @Override
  public void truncate(String name) throws Exception {
    service.dropTable(name);
    create(name);
  }

  @Override
  public void drop(String name) throws Exception {
    service.dropTable(name);
  }

  @Override
  public void dropAll() throws Exception {
    // hack: we know that all queues stored in one table
    service.dropTable(QUEUE_TABLE_NAME);
  }
}
