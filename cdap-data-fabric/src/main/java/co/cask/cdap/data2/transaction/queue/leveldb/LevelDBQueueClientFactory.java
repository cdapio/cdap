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

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueConsumer;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueEvictor;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;

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
  private static final int MAX_EVICTION_THREAD_POOL_SIZE = 10;
  private static final int EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS = 60;

  private final LevelDBTableService service;
  private final ExecutorService evictionExecutor;
  private final LevelDBQueueAdmin queueAdmin;
  private final LevelDBStreamAdmin streamAdmin;

  private final ConcurrentMap<String, Object> queueLocks = Maps.newConcurrentMap();

  @Inject
  public LevelDBQueueClientFactory(LevelDBTableService service,
                                   LevelDBQueueAdmin queueAdmin,
                                   LevelDBStreamAdmin streamAdmin) throws Exception {
    this.service = service;
    this.evictionExecutor = createEvictionExecutor();
    this.queueAdmin = queueAdmin;
    this.streamAdmin = streamAdmin;
  }

  @Override
  public QueueProducer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public QueueConsumer createConsumer(QueueName queueName, ConsumerConfig consumerConfig, int numGroups)
    throws IOException {
    LevelDBQueueAdmin admin = ensureTableExists(queueName);
    LevelDBTableCore core = new LevelDBTableCore(admin.getActualTableName(queueName), service);
    // only the first consumer of each group runs eviction; and only if the number of consumers is known (> 0).
    QueueEvictor evictor = (numGroups <= 0 || consumerConfig.getInstanceId() != 0) ? QueueEvictor.NOOP :
      new LevelDBQueueEvictor(core, queueName, numGroups, evictionExecutor);
    return new LevelDBQueueConsumer(core, getQueueLock(queueName.toString()), consumerConfig, queueName, evictor);
  }

  @Override
  public QueueProducer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    LevelDBQueueAdmin admin = ensureTableExists(queueName);
    return new LevelDBQueueProducer(
      new LevelDBTableCore(admin.getActualTableName(queueName), service), queueName, queueMetrics);
  }

  /**
   * Helper method to select the queue or stream admin, and to ensure it's table exists.
   * @param queueName name of the queue to be opened.
   * @return the queue admin for that queue.
   * @throws IOException
   */
  private LevelDBQueueAdmin ensureTableExists(QueueName queueName) throws IOException {
    LevelDBQueueAdmin admin = queueName.isStream() ? streamAdmin : queueAdmin;
    try {
      // it will create table if it is missing
      admin.create(queueName);
    } catch (Exception e) {
      throw new IOException("Failed to open table " + admin.getActualTableName(queueName), e);
    }
    return admin;
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
