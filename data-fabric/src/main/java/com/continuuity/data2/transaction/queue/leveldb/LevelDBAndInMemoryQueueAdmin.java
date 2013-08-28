package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.inmemory.InMemoryQueueAdmin;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 *
 */
@Singleton
public class LevelDBAndInMemoryQueueAdmin implements QueueAdmin {
  private final LevelDBQueueAdmin levelDBQueueAdmin;
  private final InMemoryQueueAdmin inMemoryQueueAdmin;

  @Inject
  public LevelDBAndInMemoryQueueAdmin(InMemoryQueueAdmin inMemoryQueueAdmin, LevelDBQueueAdmin levelDBQueueAdmin) {
    this.inMemoryQueueAdmin = inMemoryQueueAdmin;
    this.levelDBQueueAdmin = levelDBQueueAdmin;
  }

  @Override
  public void dropAll() throws Exception {
    levelDBQueueAdmin.dropAll();
    inMemoryQueueAdmin.dropAll();
  }

  @Override
  public boolean exists(String name) throws Exception {
    QueueName queueName = QueueName.from(name.getBytes(Charsets.UTF_8));
    if (queueName.isStream()) {
      return levelDBQueueAdmin.exists(name);
    } else {
      return inMemoryQueueAdmin.exists(name);
    }
  }

  @Override
  public void create(String name) throws Exception {
    QueueName queueName = QueueName.from(name.getBytes(Charsets.UTF_8));
    if (queueName.isStream()) {
      levelDBQueueAdmin.create(name);
    } else {
      inMemoryQueueAdmin.create(name);
    }
  }

  @Override
  public void truncate(String name) throws Exception {
    QueueName queueName = QueueName.from(name.getBytes(Charsets.UTF_8));
    if (queueName.isStream()) {
      levelDBQueueAdmin.truncate(name);
    } else {
      inMemoryQueueAdmin.truncate(name);
    }
  }

  @Override
  public void drop(String name) throws Exception {
    QueueName queueName = QueueName.from(name.getBytes(Charsets.UTF_8));
    if (queueName.isStream()) {
      levelDBQueueAdmin.drop(name);
    } else {
      inMemoryQueueAdmin.drop(name);
    }
  }
}
