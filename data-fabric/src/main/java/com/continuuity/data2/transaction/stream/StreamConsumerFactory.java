/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumer}.
 */
public interface StreamConsumerFactory {

  /**
   * Creates a {@link StreamConsumer}.
   *
   * @param streamName name of the stream
   * @param namespace application namespace for the state table.
   * @param consumerConfig consumer configuration.
   * @return a new instance of {@link StreamConsumer}.
   */
  StreamConsumer create(QueueName streamName, String namespace, ConsumerConfig consumerConfig) throws IOException;

  /**
   * Deletes all consumer states for the given namespace and group ids.
   *
   * @param streamName name of the stream
   * @param namespace application namespace for the state table.
   * @param groupIds set of group id that needs to have states cleared.
   */
  void dropAll(QueueName streamName, String namespace, Iterable<Long> groupIds) throws IOException;
}
