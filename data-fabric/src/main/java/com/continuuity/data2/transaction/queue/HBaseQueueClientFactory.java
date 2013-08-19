/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 *
 */
public final class HBaseQueueClientFactory implements QueueClientFactory {

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private final HBaseAdmin admin;
  private final byte[] tableName;

  @Inject
  public HBaseQueueClientFactory(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                                 @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf) throws IOException {
    this(hConf, cConf.get(HBaseQueueConstants.ConfigKeys.QUEUE_TABLE_NAME));
  }

  public HBaseQueueClientFactory(Configuration hConf, String tableName) throws IOException {
    this(new HBaseAdmin(hConf), tableName);
  }

  public HBaseQueueClientFactory(HBaseAdmin admin, String tableName) throws IOException {
    this.admin = admin;
    this.tableName = Bytes.toBytes(tableName);
    HBaseQueueUtils.createTableIfNotExists(admin, tableName, HBaseQueueConstants.COLUMN_FAMILY,
                                           HBaseQueueConstants.MAX_CREATE_TABLE_WAIT);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    HTable consumerTable = new HTable(admin.getConfiguration(), tableName);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return new HBaseQueue2Consumer(consumerConfig, numGroups, consumerTable, queueName);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    HTable hTable = new HTable(admin.getConfiguration(), tableName);
    // TODO: make configurable
    hTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    return new HBaseQueue2Producer(hTable, queueName, queueMetrics);
  }
}
