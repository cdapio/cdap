/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseTableUtil;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.data2.transaction.queue.QueueEvictor;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.continuuity.weave.common.Threads;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class HBaseQueueClientFactory implements QueueClientFactory {

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
  private static final int MAX_EVICTION_THREAD_POOL_SIZE = 10;
  private static final int EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS = 60;

  private final Configuration hConf;
  private final String tableName;
  private final ExecutorService evictionExecutor;
  private final HBaseQueueAdmin queueAdmin;

  @Inject
  public HBaseQueueClientFactory(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                                 @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                                 HBaseQueueAdmin queueAdmin) {
    this.hConf = hConf;
    this.tableName = HBaseTableUtil.getHBaseTableName(cConf, cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME));
    this.evictionExecutor = createEvictionExecutor();
    this.queueAdmin = queueAdmin;
  }

  // for testing only
  String getHBaseTableName() {
    return this.tableName;
  }
  
  
  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    try {
      if (!queueAdmin.exists(tableName)) {
        queueAdmin.create(tableName);
      }
    } catch (Exception e) {
      throw new IOException("Failed to open queue table " + tableName, e);
    }
    if (numGroups > 0 && consumerConfig.getInstanceId() == 0) {
      return new HBaseQueue2Consumer(consumerConfig, createHTable(), queueName,
                                     new HBaseQueueEvictor(createHTable(), queueName, evictionExecutor, numGroups));
    }
    return new HBaseQueue2Consumer(consumerConfig, createHTable(), queueName, QueueEvictor.NOOP);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    try {
      if (!queueAdmin.exists(tableName)) {
        queueAdmin.create(tableName);
      }
    } catch (Exception e) {
      throw new IOException("Failed to open queue table " + tableName, e);
    }
    return new HBaseQueue2Producer(createHTable(), queueName, queueMetrics);
  }

  private ExecutorService createEvictionExecutor() {
    return new ThreadPoolExecutor(0, MAX_EVICTION_THREAD_POOL_SIZE,
                                  EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                                  new SynchronousQueue<Runnable>(),
                                  Threads.createDaemonThreadFactory("queue-eviction-%d"),
                                  new ThreadPoolExecutor.CallerRunsPolicy());
  }

  // NOTE: this is non-private only to support unit-tests. Should never be used directly.
  HTable createHTable() throws IOException {
    HTable consumerTable = new HTable(hConf, tableName);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
  }
}
