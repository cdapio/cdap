/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 *
 */
public final class HBaseQueueClientFactory implements QueueClientFactory {

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final Configuration hConf;
  private final HBaseQueueAdmin queueAdmin;
  private final HBaseStreamAdmin streamAdmin;
  private final HBaseQueueUtil queueUtil;

  @Inject
  public HBaseQueueClientFactory(Configuration hConf,
                                 QueueAdmin queueAdmin, HBaseStreamAdmin streamAdmin) {
    this.hConf = hConf;
    this.queueAdmin = (HBaseQueueAdmin) queueAdmin;
    this.streamAdmin = streamAdmin;
    this.queueUtil = new HBaseQueueUtilFactory().get();
  }

  // for testing only
  String getTableName(QueueName queueName) {
    return (queueName.isStream() ? streamAdmin : queueAdmin).getActualTableName(queueName);
  }

  // for testing only
  String getConfigTableName(QueueName queueName) {
    return (queueName.isStream() ? streamAdmin : queueAdmin).getConfigTableName();
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    HBaseQueueAdmin admin = ensureTableExists(queueName);
    HBaseConsumerStateStore stateStore = new HBaseConsumerStateStore(queueName, consumerConfig,
                                                                     createHTable(admin.getConfigTableName()));
    return queueUtil.getQueueConsumer(consumerConfig, createHTable(admin.getActualTableName(queueName)),
                                      queueName, stateStore.getState(), stateStore);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    HBaseQueueAdmin admin = ensureTableExists(queueName);
    return new HBaseQueue2Producer(createHTable(admin.getActualTableName(queueName)), queueName, queueMetrics);
  }

  /**
   * Helper method to select the queue or stream admin, and to ensure it's table exists.
   * @param queueName name of the queue to be opened.
   * @return the queue admin for that queue.
   * @throws IOException
   */
  private HBaseQueueAdmin ensureTableExists(QueueName queueName) throws IOException {
    HBaseQueueAdmin admin = queueName.isStream() ? streamAdmin : queueAdmin;
    try {
      if (!admin.exists(queueName)) {
        admin.create(queueName);
      }
    } catch (Exception e) {
      throw new IOException("Failed to open table " + admin.getActualTableName(queueName), e);
    }
    return admin;
  }

  private HTable createHTable(String name) throws IOException {
    HTable consumerTable = new HTable(hConf, name);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
  }
}
