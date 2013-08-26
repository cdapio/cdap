/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.leveldb;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableCore;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for LevelDB queue clients.
 */
public final class LevelDBQueueClientFactory implements QueueClientFactory {

  private final LevelDBOcTableService service;

  public static final String QUEUE_TABLE_NAME = "__queues";
  private final ConcurrentMap<String, Object> queueLocks = Maps.newConcurrentMap();

  @Inject
  public LevelDBQueueClientFactory(LevelDBOcTableService service) throws IOException {
    this.service = service;
    service.createTable(QUEUE_TABLE_NAME);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    return new LevelDBQueue2Consumer(new LevelDBOcTableCore(QUEUE_TABLE_NAME, service),
                                     getQueueLock(queueName.toString()),
                                     consumerConfig, queueName, QueueEvictor.NOOP);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return new LevelDBQueue2Producer(new LevelDBOcTableCore(QUEUE_TABLE_NAME, service),
                                     queueName, queueMetrics);
  }

  private Object getQueueLock(String queueName) {
    Object lock = queueLocks.get(queueName);
    if (lock == null) {
      lock = new Object();
      Object existing = queueLocks.putIfAbsent(queueName, lock);
      if (existing != null) {
        lock = existing;
      }
    }
    return lock;
  }
}
