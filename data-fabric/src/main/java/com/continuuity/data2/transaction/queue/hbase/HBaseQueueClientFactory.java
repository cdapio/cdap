/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.transaction.queue.QueueMetrics;
import com.google.inject.Inject;
import com.google.inject.name.Named;
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
  private final String tableName;
  private final HBaseQueueAdmin queueAdmin;

  @Inject
  public HBaseQueueClientFactory(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                                 HBaseQueueAdmin queueAdmin) {
    this.hConf = hConf;
    this.tableName = queueAdmin.getTableName();
    this.queueAdmin = queueAdmin;
  }

  // for testing only
  String getTableName() {
    return this.tableName;
  }

  // for testing only
  String getConfigTableName() {
    return queueAdmin.getConfigTableName();
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
    HBaseConsumerStateStore stateStore = new HBaseConsumerStateStore(queueName, consumerConfig,
                                                                     createHTable(queueAdmin.getConfigTableName()));
    return new HBaseQueue2Consumer(consumerConfig, createHTable(tableName),
                                   queueName, stateStore.getState(), stateStore);
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
    return new HBaseQueue2Producer(createHTable(tableName), queueName, queueMetrics);
  }

  private HTable createHTable(String name) throws IOException {
    HTable consumerTable = new HTable(hConf, name);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
  }
}
