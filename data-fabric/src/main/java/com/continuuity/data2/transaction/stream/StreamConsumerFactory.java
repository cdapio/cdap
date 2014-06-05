/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data2.queue.ConsumerConfig;
import com.sun.istack.Nullable;

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

}
