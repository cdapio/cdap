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
import com.continuuity.weave.common.Threads;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory for LevelDB queue clients.
 */
public final class LevelDBQueueClientFactory implements QueueClientFactory {

  public static final String QUEUE_TABLE_NAME = "__queues";

  private static final int MAX_EVICTION_THREAD_POOL_SIZE = 10;
  private static final int EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS = 60;

  private final LevelDBOcTableService service;
  private final ExecutorService evictionExecutor;

  private final ConcurrentMap<String, Object> queueLocks = Maps.newConcurrentMap();

  @Inject
  public LevelDBQueueClientFactory(LevelDBOcTableService service) throws IOException {
    this.service = service;
    this.evictionExecutor = createEvictionExecutor();
    service.createTable(QUEUE_TABLE_NAME);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    LevelDBOcTableCore core = new LevelDBOcTableCore(QUEUE_TABLE_NAME, service);
    // only the first consumer of each group runs eviction; and only if the number of consumers is known (> 0).
    QueueEvictor evictor = (numGroups <= 0 || consumerConfig.getInstanceId() != 0) ? QueueEvictor.NOOP :
      new LevelDBQueueEvictor(core, queueName, numGroups, evictionExecutor);
    return new LevelDBQueue2Consumer(core, getQueueLock(queueName.toString()), consumerConfig, queueName, evictor);
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

  private ExecutorService createEvictionExecutor() {
    return new ThreadPoolExecutor(0, MAX_EVICTION_THREAD_POOL_SIZE,
                                  EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                                  new SynchronousQueue<Runnable>(),
                                  Threads.createDaemonThreadFactory("queue-eviction-%d"),
                                  new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
