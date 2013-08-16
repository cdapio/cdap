/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.flow;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * A helper class for managing queue consumer instances.
 */
@NotThreadSafe
final class QueueConsumerSupplier implements Supplier<Queue2Consumer> {

  private final QueueClientFactory clientFactory;
  private final QueueName queueName;
  private ConsumerConfig consumerConfig;
  private Queue2Consumer consumer;

  QueueConsumerSupplier(QueueClientFactory clientFactory, QueueName queueName, ConsumerConfig consumerConfig) {
    this.clientFactory = clientFactory;
    this.queueName = queueName;
    this.consumerConfig = consumerConfig;
  }

  void updateInstanceCount(int groupSize) {
    consumerConfig = new ConsumerConfig(consumerConfig.getGroupId(),
                                        consumerConfig.getInstanceId(),
                                        groupSize,
                                        consumerConfig.getDequeueStrategy(),
                                        consumerConfig.getHashKey());
    consumer = null;
  }

  @Override
  public Queue2Consumer get() {
    try {
      if (consumer == null) {
        consumer = clientFactory.createConsumer(queueName, consumerConfig);
      }
      return consumer;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
