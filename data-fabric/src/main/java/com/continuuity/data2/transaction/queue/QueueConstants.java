/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

/**
 * Constants for queue implementation in HBase.
 */
public final class QueueConstants {

  /**
   * Configuration keys for queues in HBase.
   */
  public static final class ConfigKeys {
    public static final String QUEUE_TABLE_COPROCESSOR_DIR = "data.queue.table.coprocessor.dir";
    public static final String QUEUE_TABLE_PRESPLITS = "data.queue.table.presplits";
  }

  public static final String QUEUE_TABLE_PREFIX = "queue";
  public static final String STREAM_TABLE_PREFIX = "stream";
  public static final String QUEUE_CONFIG_TABLE_NAME = QUEUE_TABLE_PREFIX + ".config";

  public static final String DEFAULT_QUEUE_TABLE_COPROCESSOR_DIR = "/queue";
  public static final int DEFAULT_QUEUE_TABLE_PRESPLITS = 16;

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  // How frequently (in seconds) to update the ConsumerConfigCache data for the HBaseQueueRegionObserver
  public static final String QUEUE_CONFIG_UPDATE_FREQUENCY = "data.queue.config.update.interval";
  public static final Long DEFAULT_QUEUE_CONFIG_UPDATE_FREQUENCY = 5L; // default to 5 seconds

  /**
   * whether a queue is a queue or a stream.
   */
  public enum QueueType {
    QUEUE, STREAM
  }

  private QueueConstants() {
  }
}
